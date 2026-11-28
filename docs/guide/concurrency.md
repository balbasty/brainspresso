# Concurrency and shared filesystems

Several brainspresso commands can work on the same dataset at once, from one
machine or from several machines that share a filesystem. Two mechanisms keep
them from interfering.

## Run locks

Harvest and roast take a *shared* lock on the dataset, and compost takes an
*exclusive* one. Two harvests can therefore run together, but a dataset cannot
be composted while it is being harvested or roasted, and the other way round.
The second command fails at once and names the command that holds the lock.
`--lock-wait SECONDS` makes it wait instead.

Each running command also leaves a record in `.brainspresso/runs/`. Compost
refuses to start while records of other commands are present. A record can be
left behind by a command that crashed, or appear on a filesystem that does not
share locks between machines. Once you have checked that nothing else is
running, `--ignore-runs` lets compost proceed.

## File locks

Files are locked while they are read or written, using lock files in
`.brainspresso/locks/`. This works on Linux, macOS and Windows. Partial
downloads are locked too, so that two harvests never write the same file.

Some network and cluster filesystems do not support locks. On those,
brainspresso refuses to run. `BRAINSPRESSO_LOCKS=0` disables every lock,
which is only safe when no other command uses the dataset at the same time.
