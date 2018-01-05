#!/usr/bin/env node
// import {bootstrap} from './database'
import {fetchLocalMutations, Mutation} from './mutation'
import {bootstrap, fetchRemoteMutations, MutationRunner} from './database'
// import ch from 'chalk'
import * as log from './log'

async function run() {
  console.log('bootstraping')
  await bootstrap()

  const local = await fetchLocalMutations()
  const remotes = await fetchRemoteMutations()
  var error = false

  const print = Mutation.once(m => {
    // console.log(
    //   `${ch.yellowBright(m.module)}:${ch.redBright(m.name)}${m.serie ? ch.greenBright('.' + m.serie) : ''}`,
    // )
    for (var e of m.errors) { log.err(e); error = true }
  })

  for (var mut of local) {
    await mut.up(print)
  }

  if (!error) {
    const runner = new MutationRunner(local, remotes)
    await runner.mutate()
  }

  process.exit(0)
}

run().catch(e => {
  console.error(e)
  process.exit(1)
})
