# Data ingestion with Spark Streaming, Kafka and Avro schema

#### Piotr Przetacznik

## What is Kafka?

* Kafka is like REST but asynchronous?
* Kafka is like a Service Bus?
* Kafka is like a Database?

[1]: Stopford, Ben. *Designing Event-Driven Systems*. 2019. online: https://www.confluent.io/resources/ebook/designing-event-driven-systems/

## What is Kafka really?

* A streaming platform?
* Replicated Distributed Commit Log [2]
* Database but inside-out

[2]: Kreps, Jay, et al. 2011

## End of Moore Law?

* Bandwidth vs latency
* Moore's law
* Little's law
* Amdahl's law

> *You don't have to be an engineer to be be a racing driver, but you do have to have Mechanical Sympathy* ~ Jackie Stewart

## Transactional messaging - outbox pattern

**Use case**: Order service wants to make a change to the database and send a message

    * Possible message loss
    * Performing db change and saving a message to the outbox table within a single transaction

* Saga patterns

[4]: Richardson, Chris. *Microservices patterns*. 2018. online: https://microservices.io/patterns/data/transactional-outbox.html

## Use cases

* Even Sourcing
* Command Sourcing
* CQRS
* CDC
* ETL, SCD
* Schema Validation
* Security

## Exactly Once

* Two opportunities for failure
    * when sending to the broker
    * when reading from the broker

* Is exactly once possible?
    * Atomic Commit - send and acknoledgement
    * Idempotency
    * Deduplicating

## Schema compatibility types

╔═════════════════════╦══════════════════════════╦═════════════════════════════════╦═══════════════╗
║ Compatibility Type  ║ Changes allowed          ║ Check against which schemas     ║ Upgrade first ║
╠═════════════════════╬══════════════════════════╬═════════════════════════════════╬═══════════════╣
║ BACKWARD            ║ Delete fields            ║ Last version                    ║ Consumers     ║
║                     ║ Add optional fields      ║                                 ║               ║
║ BACKWARD_TRANSITIVE ║ Delete fields            ║ All previous versions           ║ Consumers     ║
║                     ║ Add optional fields      ║                                 ║               ║
║ FORWARD             ║ Add fields               ║ Last version                    ║ Producers     ║
║                     ║ Delete optional fields   ║                                 ║               ║
║ FORWARD_TRANSITIVE  ║ Add fields               ║ All previous versions           ║ Producers     ║
║                     ║ Delete optional fields   ║                                 ║               ║
║ FULL                ║ Add optional fields      ║ Last version                    ║ Any order     ║
║                     ║ Delete optional fields   ║                                 ║               ║
║ FULL_TRANSITIVE     ║ Add optional fields      ║ All previous versions           ║ Any order     ║
║                     ║ Delete optional fields   ║                                 ║               ║
║ NONE                ║ All changes are accepted ║ Compatibility checking disabled ║ Depends       ║
╚═════════════════════╩══════════════════════════╩═════════════════════════════════╩═══════════════╝


*transitive: ensures compatibility between X-2 <==> X-1 and X-1 <==> X and X-2 <==> X*
*non-transitive: ensures compatibility between X-2 <==> X-1 and X-1 <==> X, but not necessarily X-2 <==> X*


[3]: https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html#compatibility-types

## Demo

* Spark
* Kafka
* Parquet
* Avro Schema

* Case 1:
    * Adding optional fields in Producer,
    * Consumer is running for the whole time

* Case 2:
    * Publishing two types of messages to a single Kafka topic

## Thanks!
