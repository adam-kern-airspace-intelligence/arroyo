# Contributing to Arroyo

We welcome contributions from the community!

Please refer to the [dev setup](https://doc.arroyo.dev/developing/dev-setup) guide for how to get started. You can
find help on our [discord](https://discord.gg/cjCr5rVmyR) or via email at
[support@arroyo.systems](mailto:support@arroyo.systems).

## Local builds

The repository includes a `mise` configuration for the pinned build tools and
the local build workflow. Docker must be running; the build starts its local
PostgreSQL database and runs its migrations automatically.
It is published on port `5433` to avoid conflicting with other local services.

```bash
mise install
mise run build
```

To keep rebuilding while working, run `mise run build:watch`.

To stop the database, run `mise run db:down`. To remove its data as well, run
`mise run db:destroy`.

Run `mise run dev` to start the cluster and its embedded console at
`http://localhost:5115`. For frontend hot reload, run `mise run web:dev` in a
second terminal and open `http://localhost:5173`.
