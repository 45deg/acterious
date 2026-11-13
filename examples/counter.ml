open Acterious

(* 返信先のアクターもメッセージに含めて渡す。宛先の型によって送れる値の型が決まる。

   Pass the reply actor inside the message; its type constrains the reply value. *)
type message =
  | Add of int
  | Read of int Actor.t

let send actor message = if not (Actor.send actor message) then failwith "宛先は終了しています"

let () =
  Eio_main.run
  @@ fun env ->
  Eio.Switch.run
  @@ fun sw ->
  let pool = Pool.create ~sw ~domain_mgr:(Eio.Stdenv.domain_mgr env) ~domains:2 () in
  Fun.protect
    ~finally:(fun () -> Pool.shutdown pool)
    (fun () ->
       let printer =
         Actor.spawn pool ~initial:() (fun () value -> Printf.printf "合計: %d\n%!" value)
       in
       let counter =
         Actor.spawn pool ~initial:0 (fun total -> function
           | Add amount -> total + amount
           | Read reply_to ->
             send reply_to total;
             total)
       in
       send counter (Add 10);
       send counter (Add 20);
       send counter (Read printer);
       (* 先に counter の終了を待つことで、printer への返信が送信済みであることを保証してから
          printer を閉じる。

          Wait for counter to finish first so its reply to printer is guaranteed to be
          queued before printer is stopped. *)
       Actor.stop counter;
       Actor.join counter;
       Actor.stop printer;
       Actor.join printer)
;;
