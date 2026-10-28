(* ロック内では Eio の待機やユーザーのハンドラーを実行しない。
   標準 Mutex を使うことで、Eio を起動していない Domain からも送信できる。 *)
type pool = {
  jobs : (unit -> unit) Queue.t;
  mutex : Mutex.t;
  changed : Eio.Condition.t;
  mutable closed : bool;
  mutable pending : int;
  mutable workers : unit Eio.Promise.or_exn list;
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

  let shutdown pool =
    (* キャンセルされた呼び出し元からの後始末でも、受理済みの処理を完了する。
       pending はキューだけでなく I/O 待ち中のハンドラーも数える。 *)
    Eio.Cancel.protect (fun () ->
        Mutex.protect pool.mutex (fun () ->
            pool.closed <- true;
            Eio.Condition.broadcast pool.changed);
        let outcomes = List.map Eio.Promise.await pool.workers in
        List.iter (function Ok () -> () | Error error -> raise error) outcomes)

  let create ~sw ~domain_mgr ~domains () =
    if domains < 1 then invalid_arg "Pool.create: domains must be positive";
    let pool =
      {
        jobs = Queue.create ();
        mutex = Mutex.create ();
        changed = Eio.Condition.create ();
        closed = false;
        pending = 0;
        workers = [];
      }
    in
    (* 各ワーカーの開始を確認してから返す。部分的な起動失敗でも、開始済みの
       ワーカーを shutdown で回収する。 *)
    match
      for _ = 1 to domains do
        let ready, started = Eio.Promise.create () in
        let worker =
          Eio.Fiber.fork_promise ~sw (fun () ->
              Eio.Cancel.protect (fun () ->
                  match
                    Eio.Domain_manager.run domain_mgr (fun () ->
                        Eio.Promise.resolve_ok started ();
                        work pool)
                  with
                  | () -> ()
                  | exception error ->
                      if not (Eio.Promise.is_resolved ready) then
                        Eio.Promise.resolve_error started error;
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
        (try shutdown pool with _ -> ());
        Printexc.raise_with_backtrace error backtrace
end

module Actor = struct
  type 'message t = {
    pool : pool;
    mailbox : 'message Queue.t;
    handle : 'message -> unit;
    mutable scheduled : bool;
    mutable closed : bool;
    mutable failure : (exn * Printexc.raw_backtrace) option;
  }

  (* ワーカーから呼び出され、メッセージを 1 件だけ処理する。
     scheduled は実行待ちキューへの登録から処理完了まで true のまま維持する。 *)
  let rec run actor () =
    let pool = actor.pool in
    let message =
      Mutex.protect pool.mutex (fun () -> Queue.take actor.mailbox)
    in
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
            Queue.clear actor.mailbox);
        (* メールボックスに残りがあれば scheduled を true に保ったまま実行待ちキューの末尾へ戻し、
           空なら false にして次回の send で再登録できるようにする。この判定と send を同じロックで
           保護することで、処理完了と新規送信が重なっても登録漏れを防ぐ。 *)
        if Queue.is_empty actor.mailbox then (
          actor.scheduled <- false;
          pool.pending <- pool.pending - 1)
        else Queue.add (run actor) pool.jobs;
        Eio.Condition.broadcast pool.changed)

  (* 必ず pool.mutex を保持した状態で呼ぶこと。
     scheduled が true ならすでに実行待ちか実行中であるため、重複して登録しない。 *)
  let schedule actor =
    if not actor.scheduled then (
      actor.scheduled <- true;
      actor.pool.pending <- actor.pool.pending + 1;
      Queue.add (run actor) actor.pool.jobs;
      (* ワーカーと join の待機 Fiber に、状態の再確認を促す。 *)
      Eio.Condition.broadcast actor.pool.changed)

  let spawn pool ~initial handle =
    (* 状態の更新を handle フィールドのクロージャに閉じ込める。同時実行は scheduled で防ぎ、
       更新された状態は Mutex を介して次のワーカーへ引き渡される。 *)
    let state = ref initial in
    Mutex.protect pool.mutex (fun () ->
        if pool.closed then invalid_arg "Actor.spawn: pool is closed";
        {
          pool;
          mailbox = Queue.create ();
          handle = (fun message -> state := handle !state message);
          scheduled = false;
          closed = false;
          failure = None;
        })

  let send actor message =
    Mutex.protect actor.pool.mutex (fun () ->
        if actor.closed || actor.pool.closed then false
        else (
          Queue.add message actor.mailbox;
          schedule actor;
          true))

  let stop actor =
    Mutex.protect actor.pool.mutex (fun () ->
        actor.closed <- true;
        Eio.Condition.broadcast actor.pool.changed)

  let join actor =
    let failure =
      Eio.Condition.loop_no_mutex actor.pool.changed (fun () ->
          Mutex.protect actor.pool.mutex (fun () ->
              if actor.scheduled || not (actor.closed || actor.pool.closed) then
                None
              else Some actor.failure))
    in
    match failure with
    | None -> ()
    | Some (error, backtrace) -> Printexc.raise_with_backtrace error backtrace
end
