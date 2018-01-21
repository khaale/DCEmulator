module DCEmulator.App

open Akkling
open Actors
open Common
open System.Threading
open Common.Utils

let cfg = { 
            NodesCount = 10; 
            TxCount = 100;
            ConnectivityProbability = 0.1; 
            MaliciousNodeProbabillity = 0.15; 
            TxCommunicationProbablity = 0.05;
            RoundsCount = 1000;
        }

[<EntryPoint>]
let main argv =
    use system = System.create "my-system" <| Configuration.defaultConfig()
    
    let nodeFactory s i t = 
        match Rnd.randomDecision cfg.MaliciousNodeProbabillity with
        | true -> NodeImpl.maliciousNode cfg s i t
        | false -> NodeImpl.compliantNode cfg s i t
    
    let masterRef = masterActor system nodeFactory cfg

    let rec waitForConsensus () = 
        let (state':Map<string,Tx array> option) = masterRef <? MasterMsg.QueryState |> Async.RunSynchronously
        match state' with 
        | Some s -> s
        | None ->
            Thread.Sleep(100)
            waitForConsensus ()
    let consensusState = waitForConsensus () 
    system.Terminate() |> Async.AwaitTask |> Async.RunSynchronously

    printfn "%s" (sprintConsensus consensusState)
    0 // return exit code