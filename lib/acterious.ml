(** 固定数の Domain 上で型付き Actor を動かす、学習用フレームワーク。 *)

(* 末尾のシグネチャで公開 API を限定し、内部の型と補助関数を隠す。 *)
include (
struct
  (* ロック内では Eio の待機やユーザーのハンドラーを実行しない。
     標準 Mutex を使うことで、Eio を起動していない Domain からも送信できる。 *)
  type pool =
    { jobs : (unit -> unit) Queue.t
    ; mutex : Mutex.t
    ; changed : Eio.Condition.t
    ; mutable closed : bool
    ; (* 実行待ちと処理中（I/O 待ちを含む）の Actor 数。 *)
      mutable pending : int
    ; mutable workers : unit Eio.Promise.or_exn list
    }

  module Pool = struct
    type t = pool

    let work pool =
      Eio.Switch.run (fun sw ->
        let rec dispatch () =
          let job =
            (* 通知の登録後にロック内で状態を調べるため、確認と待機の間に
               別 Domain から届いた通知も取りこぼさない。 *)
            Eio.Condition.loop_no_mutex pool.changed (fun () ->
              Mutex.protect pool.mutex (fun () ->
                match Queue.take_opt pool.jobs with
                | Some job -> Some (Some job)
                | None when pool.closed && pool.pending = 0 -> Some None
                | None -> None))
          in
          match job with
          | None -> ()
          | Some run ->
            Eio.Fiber.fork ~sw run;
            Eio.Fiber.yield ();
            dispatch ()
        in
        dispatch ())
    ;;

    let shutdown pool =
      (* キャンセルされた呼び出し元からの後始末でも、受理済みの処理を完了する。
         ハンドラーが待機し続けると shutdown も戻らない。 *)
      Eio.Cancel.protect (fun () ->
        Mutex.protect pool.mutex (fun () ->
          pool.closed <- true;
          Eio.Condition.broadcast pool.changed);
        (* Actor ごとの失敗はここでは送出しない。成否は Actor.join で確認する。 *)
        let outcomes = List.map Eio.Promise.await pool.workers in
        List.iter
          (function
            | Ok () -> ()
            | Error error -> raise error)
          outcomes)
    ;;

    let create ~sw ~domain_mgr ~domains () =
      if domains < 1 then invalid_arg "Pool.create: domains must be positive";
      let pool =
        { jobs = Queue.create ()
        ; mutex = Mutex.create ()
        ; changed = Eio.Condition.create ()
        ; closed = false
        ; pending = 0
        ; workers = []
        }
      in
      (* 各ワーカーの開始を確認してから返す。部分的な起動失敗でも、開始済みの
         ワーカーを shutdown で回収する。 *)
      match
        for _ = 1 to domains do
          let ready, started = Eio.Promise.create () in
          let worker =
            Eio.Fiber.fork_promise ~sw (fun () ->
              (* 受理済みの処理を完遂するため親のキャンセルを遮蔽する。
                 sw の終了だけでは停止しないので、明示的な shutdown が必要。 *)
              Eio.Cancel.protect (fun () ->
                match
                  Eio.Domain_manager.run domain_mgr (fun () ->
                    Eio.Promise.resolve_ok started ();
                    work pool)
                with
                | () -> ()
                | exception error ->
                  if not (Eio.Promise.is_resolved ready)
                  then Eio.Promise.resolve_error started error;
                  raise error))
          in
          pool.workers <- worker :: pool.workers;
          Eio.Promise.await_exn ready
        done
      with
      | () -> pool
      | exception error ->
        let backtrace = Printexc.get_raw_backtrace () in
        (* 起動失敗したワーカーの例外も回収し、元の失敗を保持する。 *)
        (try shutdown pool with
         | _ -> ());
        Printexc.raise_with_backtrace error backtrace
    ;;
  end

  module Actor = struct
    type 'message t =
      { pool : pool
      ; mailbox : 'message Queue.t
      ; handle : 'message -> unit
      ; (* キューへの登録から処理完了まで true。待機中も後続メッセージを開始させない。 *)
        mutable scheduled : bool
      ; mutable closed : bool
      ; mutable failure : (exn * Printexc.raw_backtrace) option
      }

    (* ワーカーから呼び出され、メッセージを 1 件だけ処理する。 *)
    let rec run actor () =
      let pool = actor.pool in
      let message = Mutex.protect pool.mutex (fun () -> Queue.take actor.mailbox) in
      (* ハンドラーの未捕捉例外はアクター内に保存してワーカーへは伝播させず、
         失敗したアクターだけを終了させて他のアクターの処理を継続する。 *)
      let outcome =
        match actor.handle message with
        | () -> Ok ()
        | exception error -> Error (error, Printexc.get_raw_backtrace ())
      in
      Mutex.protect pool.mutex (fun () ->
        (match outcome with
         | Ok () -> ()
         | Error failure ->
           actor.failure <- Some failure;
           actor.closed <- true;
           (* ハンドラー失敗時に限り、受理済みでも未処理のメッセージを破棄する。 *)
           Queue.clear actor.mailbox);
        (* send と同じロックで完了を判定し、新規送信との競合による登録漏れを防ぐ。 *)
        if Queue.is_empty actor.mailbox
        then (
          (* 次の send で再登録できるようにし、処理対象の Actor 数を減らす。 *)
          actor.scheduled <- false;
          pool.pending <- pool.pending - 1)
        else
          (* scheduled を true に保ち、残りはキュー末尾に戻して他の Actor に順番を譲る。 *)
          Queue.add (run actor) pool.jobs;
        Eio.Condition.broadcast pool.changed)
    ;;

    (* 必ず pool.mutex を保持した状態で呼ぶこと。
       scheduled が true ならすでに実行待ちか実行中であるため、重複して登録しない。 *)
    let schedule actor =
      if not actor.scheduled
      then (
        actor.scheduled <- true;
        actor.pool.pending <- actor.pool.pending + 1;
        Queue.add (run actor) actor.pool.jobs;
        (* ワーカーと join の待機 Fiber に、状態の再確認を促す。 *)
        Eio.Condition.broadcast actor.pool.changed)
    ;;

    let spawn pool ~initial handle =
      (* 状態の更新を handle フィールドのクロージャに閉じ込める。同時実行は scheduled で防ぎ、
         更新された状態は Mutex を介して次のワーカーへ引き渡される。 *)
      let state = ref initial in
      Mutex.protect pool.mutex (fun () ->
        if pool.closed then invalid_arg "Actor.spawn: pool is closed";
        { pool
        ; mailbox = Queue.create ()
        ; handle = (fun message -> state := handle !state message)
        ; scheduled = false
        ; closed = false
        ; failure = None
        })
    ;;

    let send actor message =
      Mutex.protect actor.pool.mutex (fun () ->
        if actor.closed || actor.pool.closed
        then false
        else (
          (* 同一送信元の順序を保ち、同時送信はロック取得順に並ぶ。 *)
          Queue.add message actor.mailbox;
          schedule actor;
          true))
    ;;

    let stop actor =
      Mutex.protect actor.pool.mutex (fun () ->
        actor.closed <- true;
        Eio.Condition.broadcast actor.pool.changed)
    ;;

    let join actor =
      let failure =
        Eio.Condition.loop_no_mutex actor.pool.changed (fun () ->
          Mutex.protect actor.pool.mutex (fun () ->
            if actor.scheduled || not (actor.closed || actor.pool.closed)
            then None
            else Some actor.failure))
      in
      match failure with
      | None -> ()
      (* 失敗は消費せず、繰り返し join しても元の例外とバックトレースを再送出する。 *)
      | Some (error, backtrace) -> Printexc.raise_with_backtrace error backtrace
    ;;
  end
end :
sig
  module Pool : sig
    (** 固定数のワーカー Domain を持つプール。 *)
    type t

    (** [domains] 個のワーカーを [sw] 内に作る。[domains < 1] は [Invalid_argument]。
        各ワーカーで Eio が動き、Actor を増やしても Domain 数は増えない。
        Eio の Fiber 上で呼び、[sw] を抜ける前に [Fun.protect] の
        [finally] で必ず [shutdown] すること。 *)
    val create
      :  sw:Eio.Switch.t
      -> domain_mgr:[> Eio.Domain_manager.ty ] Eio.Domain_manager.t
      -> domains:int
      -> unit
      -> t

    (** 全 Actor への送信と新規起動を締め切り、受理済みの処理とワーカーの終了を待つ。
        Eio の Fiber 上で呼ぶ。複数回呼んでも安全。
        同じプールのハンドラー内からは呼ばないこと。 *)
    val shutdown : t -> unit
  end

  module Actor : sig
    (** ['message] 型のメッセージを受け取る宛先。 *)
    type 'message t

    (** 初期状態と、状態・メッセージから次の状態を返すハンドラーで起動する。
        閉じたプールでは [Invalid_argument]。Eio の外の Domain からも呼べる。
        状態とメッセージはコピーしないため、外部と可変データを共有する場合は同期が必要。 *)
    val spawn : Pool.t -> initial:'state -> ('state -> 'message -> 'state) -> 'message t

    (** 追加に成功すれば [true]、Actor またはプールが閉じていれば [false]。
        [true] は処理完了を意味しない。メールボックスに容量制限はなく、空き待ちはしない。
        Eio の外も含め複数 Domain から送信でき、待機は短い Mutex の取得だけ。 *)
    val send : 'message t -> 'message -> bool

    (** 新規送信を締め切る。実行中のハンドラーは中断せず、受理済みの処理も待たない。
        Eio の外からも呼べて、複数回呼んでも安全。 *)
    val stop : 'message t -> unit

    (** 閉鎖後の全処理完了を Eio の Fiber 上で待つ。通常は先に [stop] する。
        待機中は他 Fiber が進み、呼び出し元のキャンセルでも Actor は停止しない。
        ハンドラーの失敗は元のバックトレースとともに再送出する。
        自己待ちや循環待ちを避けるため、ハンドラー内からは呼ばないこと。 *)
    val join : 'message t -> unit
  end
end)
