## Controller library

This library provides a wrapper to enable Controllers interface with NATS
and abstract away dealing with retrieving a Condition, execute the registered handler
and publish status updates.

It covers,

- Interfacing with Jetstream, KV and dealing with keep alive sematics
- Listening for `Conditions` on either NATS directly or through an HTTPs endpoint provided by the [Orchestator API](https://github.com/metal-toolbox/conditionorc/blob/2a62f9762092803b9959e1aa78bcac9a0f7392b8/pkg/api/v1/orchestrator/routes/routes.go#L171)
- Publishes the first status before executing the Controller handler method
- Invokes the Controller handler, passing it a `Task` object (Controllers don't know and don't care about `Conditions`)
- Covers up for any failures or panics in the controller Handler
- Collect trace telemetry and metrics for Conditions handled

### Projects using ctrl

This Controller `ctrl` library is imported by Controllers interfacing with the
[Condition Orchestrator](https://github.com/metal-toolbox/conditionorc).

- [flipflop](https://github.com/metal-toolbox/flipflop)
- [flasher](https://github.com/metal-toolbox/flasher)
