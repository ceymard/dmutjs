
import {Client, QueryResult} from 'pg'
import {Mutation} from './mutation'

import chalk from 'chalk'
const ch = chalk.constructor({level: 3})

const schema = `"dmut"`
const table = `"mutations"`
const tbl = `${schema}.${table}`


/**
 * Dmut mutations are always the first, since they create the table the mutations
 * will be stored in.
 */
export const dmut_mutation = Mutation.static
.auto `create schema ${schema}`
.auto `create table ${tbl} (
  "hash" text primary key,
  "statements" text[],
  "undo" text[],
  "children" text[],
  "static" boolean,
  "date_applied" Timestamp default now()
)`
.comment `on schema ${schema} is 'The schema holding informations about mutations.'`
.comment `on column ${tbl}."hash" is 'A unique hash identifying the mutation'`
.comment `on column ${tbl}."statements" is 'The list of statements that were applied in this mutation'`
.comment `on column ${tbl}."undo" is 'The statements that would be run if the mutation was abandoned'`
.comment `on column ${tbl}."children" is 'The list of hashes of mutations that should be downed before this one'`
.comment `on column ${tbl}."static" is 'Wether this mutation is static, as in it must not be undoed'`
.comment `on column ${tbl}."date_applied" is 'The timestamp at which this mutation was applied to the database'`


export interface HasHash {
  hash: string
}


export interface MutationRow {
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
    public client: Client
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

  without<T extends MutationRow|Mutation>(reg: T[], _hashes: string[]) {
    const hashes = new Set<string>(_hashes)
    const res: T[] = []
    const excluded: T[] = []
    for (var r of reg) {
      if (hashes.has(r.hash)) {
        excluded.push(r)
        for (var c of r.children) {
          hashes.add(c instanceof Mutation ? c.hash : c)
        }
      } else {
        res.push(r)
      }
    }
    return [res, excluded]
  }

  /**
   * Perform the mutations
   */
  async mutate(local = Mutation.registry) {

    const dbmut = await this.fetchRemoteMutations()
    // const dbdct = this.mkdct(dbmut)
    const dct = this.mkdct(local) // a dictionary of local mutations

    const invalid_hashes = [] as string[]
    // Compute the hashes that are in db, but not in local anymore.
    // These are the base mutations that need to go.
    for (var d of dbmut)
      if (!dct[d.hash])
        invalid_hashes.push(d.hash)

    // With this list of hashes, get the definite list of mutations
    // that need to be undone (these include children of the marked mutations
    // we already had), and a list of the mutations that will stay and should
    // not be re-upped.
    const [stay, leave] = this.without(dbmut, invalid_hashes)
    leave.reverse()
    const still_there = this.mkdct(stay)

    if (!this.testing) await this.query('begin')
    try {

      for (var rm of leave) {
        // if (rm.static)
        //   throw new Error(`cannot undo a static mutation, yet ${rm.hash} is no longer here`)

        // LOG that we're destroying a mutation ?
        console.log(`  « ${ch.redBright(rm.hash)}`)
        for (var undo of rm.undo) {
          await this.query(undo)
        }

        await this.query(`delete from ${tbl} where hash = $1`, [rm.hash])
      }

      // Local is always in the good order, since children cannot be declared
      // before their parents.
      const to_apply: Mutation[] = local
      for (var t of to_apply) {
        if (still_there[t.hash]) continue
        console.log(`  » ${ch.greenBright(t.hash)}`)

        for (var stmt of t.statements) {
          // console.log(stmt)
          await this.query(stmt)
        }

        await this.query(`insert into ${tbl}(hash, statements, undo, children, static)
          values($1, $2, $3, $4, $5)`,
          [t.hash, t.statements, t.undo, Array.from(t.children).map(c => c.hash), t.static]
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
   * @param mutations
   */
  async test(mutations = Mutation.registry) {
    // At this point, we already mutated all of our local mutations.
    // We will now try to remove them one by one and see if they hold
    this.testing = true

    console.log(`\n--- now testing mutations---\n`)
    var errored = false
    for (var m of mutations) {
      // Not testing the basic dmut mutation
      if (m.hash === dmut_mutation.hash) continue

      try {
        // Whenever we get to this point, we can consider that all local mutations
        // are up. As such, we want to track the down mutations that were applied
        // to reapply them, and them only.

        console.log(ch.greenBright(`  ==> Testing removal of ${m.hash}`))
        await this.query('savepoint "dmut-testing"')

        // Try removing this mutation from our local list
        const [local] = this.without(mutations, [m.hash])

        // First mutate while having removed the mutation
        await this.mutate(local)

        // Then re-mutate with all of them
        await this.mutate(mutations)

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
