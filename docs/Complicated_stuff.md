### Approaches that we could possibly take wrt the new suggestions received from Tarun during iteration-3:

1. Approach 01 :
- Sanjula’s container reads Kafka and writes flattened Parquet files to a shared hard drive. delta-lakehouse container monitors that hard drive, reads the Parquet files, and runs the Delta MERGE.
- ++ if delta-lakehouse crashes, delta-processing doesnt care about it, continues its work, writing data to paraquet, lakehouse boots back up, catches up with the stuff that is there using the SUCESS marker approach.
- ++ 0 consideration about the merge conflicts, I write code wrt my container, sanjula writes her own code wrt her container.
- -- like tarun had mentioned, writing paraquet files twice to disk is actually a lot costlier, Disk I/o reads becomes costly wrt time and compute.
- -- the polling overhead is also there wrt SUCCESS Marker approach, so I feel apporoach 01 might not be the most pleasing one.

2. Approach 02:
- Sanjula's procesing logic, my deduplication logic , all packed into a single spark session. data flow is now physically equivalent to (kafka->RAM->Delta lake)
- ++ space wise, it is superior to Approach 01, we would be eliminating half of the paraquet files.
- ++ data technically never leaves the spark session, so data is always within the RAM, end to end latency decreases.
- -- A single line of wrong logic by either one of us, the major part of our pipeline crashes and we would have to worry about so many things.
- -- Scaling is kinda dependent on each other, for independent scaling, we technically have less freedom here, resouce contention is the problem i.e, at a time "i" , if a massive spike of data arrives, the CPU will prioritize the "active" task. If the CPU is 100% busy with deduplication, the initial logic that we had defined would start to lag.

3. Approach 03:
- A workaround for approach 01 you could say, we still keep two containers, divert the traffic to kafka instead i.e, the flattened df out of sanjula's container to a new topic called named XYZ, my container would then be programmed to get the data from topic XYZ, directly from kafka.
- ++Total fault isolation is maintained.
- ++ No Disk I/O, expecting reduced latency compared to Approach 01.
- -- Kafka becomes the riskiest part of project, if kafka fails, three parts of pipeline go down ( massive team collab required, I'm a bit hesitant regarding this ).
- -- Two separate kafka streams, two separate offsets , twice the headache for person handling that part.
- imo, beats approach 01 and approach 02.

4. Approach 04:
- Sanjula reads from Kafka, flattens the data, and uses Spark Structured Streaming to write directly to a Bronze Delta Table (Append-Only) on the shared volume. I read from there.
- ++ delta transaction log automatically acts as msg queue, my part sees the new data, and writes it directly to refined table.
- ++ No Airflow scheduler for both separately, no polling scripts.
- ++ 100% decoupled containers, developer friendly. 
- ++ Spark natively streams from Delta to Delta. 0 polling overhead.
- ++ data resilience handled , wouldnt have to worry about so many other stuff.
- -- Stil writing to the disk twice, so again, the least favourable downside.

#### Some other insane approaches that clearly doesnt fit here:
- using a event broker like minio to facilitate movt between these two containers.
- use a API or HTTP POST request to send the data from container 1 to container 2 , absolute bs idea if we are focusing on low latency. 

-- My order of preference : 4>2>3>1 
-- first version : 18th march by Manthan

