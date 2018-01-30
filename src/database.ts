
import {Client, QueryResult} from 'pg'
import {Mutation} from './mutation'
import {dmut_mutation, MutationRegistry, tbl} from './mutationset'

import chalk from 'chalk'
const ch = chalk.constructor({level: 3})


export interface HasHash {
  hash: string
}


export interface MutationRow {
  identifier: string
  hash: string
  statements: string[]
  undo: string[]
  children: string[]
  static: boolean
  date_applied: Date

  // True if this mutation should be kept
  keep?: boolean
}


export class MutationRunner {

  testing = false

  constructor(
    public client: Client,
    public registry: MutationRegistry
  ) {

  }

  async query(stmt: string, args?: any): Promise<QueryResult> {
    // console.log(stmt)
    try {
      if (process.env.VERBOSE)
        console.log(`  ${ch.greenBright('>>')} ${ch.grey(stmt)}`)

      const res = await this.client.query(stmt, args)
      return res
    } catch (e) {
      console.log(`  ${ch.redBright(e.message)}`)
      console.log(`${ch.grey.bold('On statement:')}\n  ${ch.grey(stmt)}`)
      throw e
    }
  }

  /**
   * Fetch the hashes of the mutations we have in database.
   */
  async fetchRemoteMutations(): Promise<MutationRow[]> {
    try {
      const res = await this.client.query(`select * from ${tbl} order by date_applied`)
      return res.rows as MutationRow[]
    } catch {
      return []
    }
  }

  mkdct<T extends HasHash>(reg: T[]): {[hash: string]: T} {
    const res = {} as {[hash: string]: T}
    for (var r of reg) {
      res[r.hash] = r
    }
    return res
  }

  /**
   * Perform the mutations
   */
  async mutate(registry = this.registry) {

    const dbmut = await this.fetchRemoteMutations()
    // const dbdct = this.mkdct(dbmut)
    const dct = this.mkdct(registry.mutations) // a dictionary of local mutations

    // These mutations will have to go
    const gone = [] as MutationRow[]

    // But these will stay.
    const staying = [] as MutationRow[]

    for (var d of dbmut) {
      if (!dct[d.hash]) {
        gone.push(d)
      } else {
        staying.push(d)
      }
    }

    // We have to de-apply mutations in reverse order
    gone.reverse()

    // This will be used to avoid upping a local mutation.
    const still_there = this.mkdct(staying)

    if (!this.testing) await this.query('begin')
    try {

      for (var rm of gone) {
        // if (rm.static)
        //   throw new Error(`cannot undo a static mutation, yet ${rm.hash} is no longer here`)

        // LOG that we're destroying a mutation ?
        console.log(`  « ${ch.redBright(rm.identifier || rm.hash)}`)
        for (var undo of rm.undo) {
          await this.query(undo)
        }

        await this.query(`delete from ${tbl} where hash = $1`, [rm.hash])
      }

      // Local is always in the good order, since children cannot be declared
      // before their parents.
      const to_apply: Mutation[] = registry.mutations
      for (var t of to_apply) {
        if (still_there[t.hash]) continue
        console.log(`  » ${ch.greenBright(t.identifier || t.hash)}`)

        for (var stmt of t.statements) {
          // console.log(stmt)
          await this.query(stmt)
        }

        await this.query(`insert into ${tbl}(identifier, hash, statements, undo, children, static)
          values($1, $2, $3, $4, $5, $6)`,
          [t.identifier, t.hash, t.statements, t.undo, Array.from(t.children).map(c => c.hash), t.static]
        )
      }

      // Once we're done, we might want to commit...
      // await query('rollback')
      if (!this.testing) {
        await this.test()
        await this.query('commit')
      }

    } catch (e) {
      if (!this.testing) {
        console.log(`Rolling back all of it since we have an error`)
        await this.query('rollback')
      }

      console.error(e.message)
      throw e
    }
  }


  /**
   *
   * @param registry
   */
  async test(registry = this.registry) {
    // At this point, we already mutated all of our local mutations.
    // We will now try to remove them one by one and see if they hold
    this.testing = true

    console.log(`\n--- now testing mutations---\n`)
    var errored = false
    for (var m of registry.mutations) {
      // Not testing the basic dmut mutation
      if (m.hash === dmut_mutation.hash) continue

      try {
        // Whenever we get to this point, we can consider that all local mutations
        // are up. As such, we want to track the down mutations that were applied
        // to reapply them, and them only.

        console.log(ch.greenBright(`  ==> Testing removal of ${m.identifier || m.hash}`))
        await this.query('savepoint "dmut-testing"')

        // Try removing this mutation from our local list
        const local = registry.without([m.hash])

        // First mutate while having removed the mutation
        await this.mutate(local)

        // Then re-mutate with all of them
        await this.mutate(registry)

      } catch(e) {
        // console.log('ERRORS ERRORS')
        errored = true
      } finally {
        // console.log('rolling back...')
        await this.query('rollback to savepoint "dmut-testing"')
      }
    }

    this.testing = false
    if (errored) throw new Error(`Mutations had errors, bailing.`)
  }

}
