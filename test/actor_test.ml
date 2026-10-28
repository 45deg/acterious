open Acterious

let check condition message = if not condition then failwith message

let send actor message = check (Actor.send actor message) "送信が受理されませんでした"

let finish actor =
  Actor.stop actor;
  Actor.join actor

(* sleep に頼らず、テスト側とハンドラーの実行順を決めるための通知。 *)
let blocking_event () =
  let mutex = Mutex.create () in
  let condition = Condition.create () in
  let signaled = ref false in
  let wait () =
    Mutex.protect mutex (fun () ->
        while not !signaled do
          Condition.wait condition mutex
        done)
  in
  let signal () =
    Mutex.protect mutex (fun () ->
        signaled := true;
        Condition.signal condition)
  in
  (wait, signal)

let event () =
  let promise, resolver = Eio.Promise.create () in
  ( (fun () -> Eio.Promise.await promise),
    fun () -> Eio.Promise.resolve resolver () )

let test_fifo_and_drain pool =
  let received = ref [] in
  let actor =
    Actor.spawn pool ~initial:() (fun () message ->
        received := message :: !received)
  in
  List.iter (send actor) [ 1; 2; 3 ];
  finish actor;
  (* ref はハンドラーだけが変更し、テスト側は join 後に読む。 *)
  check (List.rev !received = [ 1; 2; 3 ]) "終了時の FIFO 処理に失敗"

let test_state pool =
  let totals = ref [] in
  let actor =
    Actor.spawn pool ~initial:0 (fun total amount ->
        let total = total + amount in
        totals := total :: !totals;
        total)
  in
  List.iter (send actor) [ 10; 20; -5 ];
  finish actor;
  check (List.rev !totals = [ 10; 30; 25 ]) "状態が引き継がれていません"

let test_empty_stop pool =
  let actor = Actor.spawn pool ~initial:() (fun () () -> ()) in
  Actor.stop actor;
  Actor.stop actor;
  check (not (Actor.send actor ())) "stop 後に送信が受理されました";
  Actor.join actor

let test_stop_during_handler pool =
  let wait_started, signal_started = event () in
  let wait_continue, signal_continue = event () in
  let received = ref [] in
  let actor =
    Actor.spawn pool ~initial:() (fun () message ->
        if message = 1 then (
          signal_started ();
          wait_continue ());
        received := message :: !received)
  in
  send actor 1;
  wait_started ();
  (* ハンドラーが待機中でも、送信と stop はロックに阻まれない。 *)
  send actor 2;
  Actor.stop actor;
  let rejected = not (Actor.send actor 3) in
  signal_continue ();
  Actor.join actor;
  check rejected "処理中の stop 後に送信が受理されました";
  check (List.rev !received = [ 1; 2 ]) "stop が受信済みの処理を打ち切りました"

let test_concurrent_senders pool =
  let received = ref [] in
  let actor =
    Actor.spawn pool ~initial:() (fun () message ->
        received := message :: !received)
  in
  let senders =
    List.init 4 (fun sender ->
        Domain.spawn (fun () ->
            for index = 0 to 199 do
              send actor (sender, index)
            done))
  in
  List.iter Domain.join senders;
  finish actor;
  let messages = List.rev !received in
  check (List.length messages = 800) "メッセージの欠落または重複";
  for sender = 0 to 3 do
    let indices =
      List.filter_map
        (fun (source, index) -> if source = sender then Some index else None)
        messages
    in
    check (indices = List.init 200 Fun.id) "同じ送信元のメッセージ順序が壊れました"
  done

exception Handler_failed

let test_failure pool =
  let wait_started, signal_started = event () in
  let wait_fail, signal_fail = event () in
  let processed = ref [] in
  let actor =
    Actor.spawn pool ~initial:() (fun () message ->
        if message = 1 then (
          signal_started ();
          wait_fail ();
          raise Handler_failed);
        processed := message :: !processed)
  in
  send actor 1;
  wait_started ();
  send actor 2;
  signal_fail ();
  (match Actor.join actor with
  | () -> failwith "ハンドラーの例外が伝わりませんでした"
  | exception Handler_failed -> ());
  check (!processed = []) "例外終了後もメッセージを処理しています";
  check (not (Actor.send actor 3)) "例外終了後に送信が受理されました";
  (* 失敗したアクターと同じプールを、そのまま使える。 *)
  let received = ref false in
  let healthy = Actor.spawn pool ~initial:() (fun () () -> received := true) in
  send healthy ();
  finish healthy;
  check !received "アクターの例外でワーカーが失われました"

let test_shared_worker pool =
  let domains = ref [] in
  let actors =
    List.init 200 (fun _ ->
        Actor.spawn pool ~initial:() (fun () () ->
            domains := Domain.self () :: !domains))
  in
  List.iter (fun actor -> send actor ()) actors;
  List.iter finish actors;
  (* このテストは 1 ワーカーで動かすので、共有 ref の書き手も 1 つ。 *)
  check (List.length !domains = 200) "全アクターが実行されていません";
  check
    (List.length (List.sort_uniq compare !domains) = 1)
    "アクターごとに別の Domain が使われています";
  check
    (List.for_all (fun domain -> domain <> Domain.self ()) !domains)
    "メイン Domain で処理しています"

let test_fair_turns pool =
  let wait_started, signal_started = blocking_event () in
  let wait_continue, signal_continue = blocking_event () in
  let blocker =
    Actor.spawn pool ~initial:() (fun () () ->
        signal_started ();
        wait_continue ())
  in
  send blocker ();
  wait_started ();
  let order = ref [] in
  let busy = Actor.spawn pool ~initial:() (fun () n -> order := n :: !order) in
  let other = Actor.spawn pool ~initial:() (fun () n -> order := n :: !order) in
  (* ワーカーを待たせている間に、確定した順序でキューを作る。 *)
  List.iter (send busy) [ 1; 2; 3 ];
  send other 99;
  signal_continue ();
  finish blocker;
  List.iter finish [ busy; other ];
  check (List.rev !order = [ 1; 99; 2; 3 ]) "1 メッセージごとに他のアクターへ番を渡していません"

let test_parallel_actors pool =
  let wait_started, signal_started = blocking_event () in
  let wait_continue, signal_continue = blocking_event () in
  let first_domain = ref None in
  let second_domain = ref None in
  let first =
    Actor.spawn pool ~initial:() (fun () () ->
        first_domain := Some (Domain.self ());
        signal_started ();
        wait_continue ())
  in
  let second =
    Actor.spawn pool ~initial:() (fun () () ->
        second_domain := Some (Domain.self ()))
  in
  send first ();
  wait_started ();
  send second ();
  (* first が待っていても、別のワーカーが second を完了できる。 *)
  Fun.protect ~finally:signal_continue (fun () -> finish second);
  finish first;
  check (!first_domain <> !second_domain) "別アクターが並列に動いていません"

let test_shutdown pool =
  let idle = Actor.spawn pool ~initial:() (fun () () -> ()) in
  let counts = List.init 50 (fun _ -> ref 0) in
  let actors =
    List.map
      (fun count -> Actor.spawn pool ~initial:() (fun () () -> incr count))
      counts
  in
  List.iter
    (fun actor ->
      for _ = 1 to 20 do
        send actor ()
      done)
    actors;
  (* 個別に stop しなくても、メールボックスに追加済みのメッセージを処理し、
     全ワーカーを回収する。 *)
  Pool.shutdown pool;
  Pool.shutdown pool;
  List.iter Actor.join (idle :: actors);
  check (List.for_all (fun count -> !count = 20) counts) "終了時にタスクが残りました";
  check (not (Actor.send idle ())) "プール終了後に送信を受理しました";
  match Actor.spawn pool ~initial:() (fun () () -> ()) with
  | _ -> failwith "プール終了後にアクターを起動しました"
  | exception Invalid_argument _ -> ()

(* 同じ Domain 上で待機中のハンドラーを追い越せるが、同じ Actor の
   次のメッセージは追い越せないことを確認する。 *)
let test_cooperative_wait pool =
  let wait_started, signal_started = event () in
  let wait_continue, signal_continue = event () in
  let received = ref [] in
  let actor =
    Actor.spawn pool ~initial:0 (fun state message ->
        if message = 1 then (
          signal_started ();
          wait_continue ());
        check (state = message - 1) "待機中に同じ Actor の処理が重なりました";
        Eio.Fiber.yield ();
        received := message :: !received;
        message)
  in
  send actor 1;
  wait_started ();
  send actor 2;
  Actor.stop actor;
  let helper = Actor.spawn pool ~initial:() (fun () () -> signal_continue ()) in
  send helper ();
  (* join はメイン Domain の Fiber にも実行機会を渡す。 *)
  Eio.Fiber.both (fun () -> Actor.join actor) (fun () -> finish helper);
  Actor.join actor;
  check (List.rev !received = [ 1; 2 ]) "待機を含む FIFO 処理に失敗"

let test_pipe_io pool =
  Eio.Switch.run @@ fun sw ->
  let source, sink = Eio_unix.pipe sw in
  let wait_reading, signal_reading = event () in
  let received = ref "" in
  let reader =
    Actor.spawn pool ~initial:() (fun () () ->
        signal_reading ();
        let input = Eio.Buf_read.of_flow source ~max_size:32 in
        received := Eio.Buf_read.line input)
  in
  let writer =
    Actor.spawn pool ~initial:() (fun () () ->
        Eio.Flow.copy_string "hello\n" sink)
  in
  send reader ();
  wait_reading ();
  send writer ();
  finish reader;
  finish writer;
  check (!received = "hello") "同じ Domain の Actor 間で pipe I/O が進みません"

let test_failure_backtrace pool =
  let original = ref "" in
  let actor =
    Actor.spawn pool ~initial:() (fun () () ->
        Printexc.record_backtrace true;
        try raise Handler_failed
        with error ->
          let trace = Printexc.get_raw_backtrace () in
          original := Printexc.raw_backtrace_to_string trace;
          Printexc.raise_with_backtrace error trace)
  in
  send actor ();
  for _ = 1 to 2 do
    match Actor.join actor with
    | () -> failwith "失敗した Actor の join が成功しました"
    | exception Handler_failed ->
        let trace =
          Printexc.get_raw_backtrace () |> Printexc.raw_backtrace_to_string
        in
        check
          (!original <> "" && String.starts_with ~prefix:!original trace)
          "join が元のバックトレースを保持していません"
  done

let test_join_yields pool =
  let actor = Actor.spawn pool ~initial:() (fun () () -> ()) in
  Eio.Fiber.both
    (fun () -> Actor.join actor)
    (fun () ->
      send actor ();
      Actor.stop actor)

let test_shutdown_waiting_handler pool =
  let wait_started, signal_started = event () in
  let wait_continue, signal_continue = event () in
  let received = ref [] in
  let actor =
    Actor.spawn pool ~initial:() (fun () message ->
        if message = 1 then (
          signal_started ();
          wait_continue ());
        received := message :: !received)
  in
  send actor 1;
  wait_started ();
  send actor 2;
  Eio.Fiber.both
    (fun () -> Pool.shutdown pool)
    (fun () ->
      (* shutdown は最初の中断より前に送信を締め切る。 *)
      check (not (Actor.send actor 3)) "shutdown 後に送信を受理しました";
      check (!received = []) "待機中の処理を待たずに終了しました";
      signal_continue ());
  Actor.join actor;
  check (List.rev !received = [ 1; 2 ]) "shutdown が待機中の処理を破棄しました"

let test_send_stop_race pool =
  let accepted = ref [] in
  let received = ref [] in
  let actor =
    Actor.spawn pool ~initial:() (fun () n -> received := n :: !received)
  in
  let sender =
    Domain.spawn (fun () ->
        for n = 1 to 1000 do
          if Actor.send actor n then accepted := n :: !accepted
        done)
  in
  Actor.stop actor;
  Domain.join sender;
  Actor.join actor;
  check (!accepted = !received) "send と stop の競合で受理済みメッセージが失われました"

let test_cancelled_join pool =
  let actor = Actor.spawn pool ~initial:() (fun () () -> ()) in
  let waiting, signal_waiting = event () in
  (try
     Eio.Switch.run (fun sw ->
         Eio.Fiber.fork ~sw (fun () ->
             signal_waiting ();
             Actor.join actor);
         waiting ();
         Eio.Switch.fail sw Handler_failed)
   with Handler_failed -> ());
  send actor ();
  finish actor

let test_cancelled_scope ~domain_mgr () =
  let processed = ref false in
  let actor = ref None in
  (try
     Eio.Switch.run (fun sw ->
         let pool = Pool.create ~sw ~domain_mgr ~domains:1 () in
         Fun.protect
           ~finally:(fun () -> Pool.shutdown pool)
           (fun () ->
             let a =
               Actor.spawn pool ~initial:() (fun () () ->
                   Eio.Fiber.yield ();
                   processed := true)
             in
             actor := Some a;
             send a ();
             Eio.Switch.fail sw Handler_failed;
             Eio.Fiber.check ()))
   with Handler_failed -> ());
  check !processed "親のキャンセルで受理済みメッセージが失われました";
  Actor.join (Option.get !actor)

let test_invalid_pool ~sw ~domain_mgr () =
  List.iter
    (fun domains ->
      match Pool.create ~sw ~domain_mgr ~domains () with
      | pool ->
          Pool.shutdown pool;
          failwith "不正な Domain 数を受理しました"
      | exception Invalid_argument _ -> ())
    [ 0; -1 ]

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let domain_mgr = Eio.Stdenv.domain_mgr env in
  Printexc.record_backtrace true;
  test_cancelled_scope ~domain_mgr ();
  Printf.printf "ok: キャンセル時の終了処理\n%!";
  test_invalid_pool ~sw ~domain_mgr ();
  Printf.printf "ok: Domain 数の検証\n%!";
  List.iter
    (fun (name, domains, test) ->
      let pool = Pool.create ~sw ~domain_mgr ~domains () in
      Fun.protect ~finally:(fun () -> Pool.shutdown pool) (fun () -> test pool);
      Printf.printf "ok: %s\n%!" name)
    [
      ("FIFO と終了時の処理", 2, test_fifo_and_drain);
      ("状態の引き継ぎ", 2, test_state);
      ("空のメールボックスの終了と送信拒否", 2, test_empty_stop);
      ("処理中の終了要求", 2, test_stop_during_handler);
      ("複数 Domain からの送信", 2, test_concurrent_senders);
      ("例外の伝播とワーカーの継続", 1, test_failure);
      ("200 アクターで 1 ワーカーを共有", 1, test_shared_worker);
      ("1 件ずつ交代", 1, test_fair_turns);
      ("別アクターの並列実行", 2, test_parallel_actors);
      ("プール全体の終了", 2, test_shutdown);
      ("1 Domain で待機を共有し Actor は直列実行", 1, test_cooperative_wait);
      ("同じ Domain の Actor 間の pipe I/O", 1, test_pipe_io);
      ("繰り返し join しても失敗とバックトレースを保持", 1, test_failure_backtrace);
      ("join 中に同じ Domain の Fiber が進む", 1, test_join_yields);
      ("shutdown 中の Promise 待機と残りの処理", 1, test_shutdown_waiting_handler);
      ("send と stop の競合", 2, test_send_stop_race);
      ("join のキャンセルは Actor を止めない", 1, test_cancelled_join);
    ]
