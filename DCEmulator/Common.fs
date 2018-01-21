module DCEmulator.Common

open System

type NetworkConfig = { 
        NodesCount: int
        TxCount: int
        ConnectivityProbability: double
        MaliciousNodeProbabillity: double
        TxCommunicationProbablity: double
        RoundsCount: int
    }

type Tx = { Id:int }

module Rnd =
    let random =
        let rand = System.Random()
        let locker = obj()
        fun () -> lock locker rand.NextDouble
    let randomDecision prob = random () < prob
    let randomDelay = random >> ((*) 10.)
    let randomInt =
        let rand = System.Random()
        let locker = obj()
        fun (max:int) -> lock locker (fun _ -> rand.Next(max))

module Array2DExt =
    let row (arr: 'T[,]) i = arr.[i..i, *] |> Seq.cast<'T>
    let rows arr = [0..(Array2D.length1 arr)-1] |> Seq.map (row arr) 
    let col (arr: 'T[,]) i = arr.[*,i..i] |> Seq.cast<'T>
    let cols arr = [0..(Array2D.length2 arr)-1] |> Seq.map (col arr)

open Array2DExt

module Utils =
    open System.Text

    let sprintSeq s f = sprintf "(%d)[%s]" (s |> Seq.length) (String.Join(",", s |> Seq.map f))
    let sprintTxs txs = sprintSeq txs (fun x -> x.Id.ToString())
    
    let sprintMatrix (m:bool [,]) =
        let sb = StringBuilder()
        rows m |> Seq.iter(fun r ->
            let chars = r |> Seq.map (fun v -> if v then 'X' else '.') |> Seq.toArray
            sb.AppendLine (String(chars)) |> ignore
        )
        sb.ToString()

    let sprintConsensus (map:Map<string,_>) = 
        let isCompliant (name:string) = not <| name.Contains("malic")
        let lst = map |> Map.toList
        let header = [
            sprintf 
                "Consensus state - %d nodes reported, %d compliant:" 
                lst.Length 
                (lst |> List.filter (fst >> isCompliant)).Length
            "| NodesCnt | TxCnt |";
            "--------------------";
        ]
        lst
        |> List.filter (fst >> isCompliant)
        |> List.groupBy snd
        |> List.map (fun (key,nodes) -> (key |> Array.length),(nodes |> List.length))
        |> List.sortByDescending snd
        |> List.map (fun (txCnt,nodesCnt) -> sprintf "|%9d |%6d |" nodesCnt txCnt)
        |> Seq.append header 
        |> String.concat Environment.NewLine