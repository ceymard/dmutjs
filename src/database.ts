
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

  constructor(
    public client: Client
  ) {

  }

  async query(stmt: string, args?: any): Promise<QueryResult> {
    try {
      if (process.env.VERBOSE) {
        console.log(`  ${ch.greenBright('>>')} ${ch.grey(stmt)}`)
      }
      return await this.client.query(stmt, args)
    } catch (e) {
      console.log(`  ${ch.redBright(e.message)}`)
      console.log(`${ch.grey.bold('On statement:')}\n  ${ch.grey(stmt)}`)
      throw e
    }
  }

  /**
   * Fetch the hashes of the mutations we have in database.
   */
  async fetchRemoteMutations(): Promise<{hash: string}[]> {
    const res = await this.client.query(`select * from ${tbl}`)

    var local_mutations = Mutation.registry
    var by_hash = local_mutations.reduce((acc, item) => {
      acc[item.hash] = item
      return acc
    }, {} as {[name: string]: Mutation})

    const results = res.rows as MutationRow[]

    for (var dm of results)
      // Determine if the database mutation is still in the Mutation array.
      dm.keep = !!by_hash[dm.hash]

    return results
  }

  async mutate() {
    await this.query('begin')

    try {

      // await this.test(this.local)
      // Once we're done, we might want to commit...
      // await query('rollback')
      await this.query('commit')
    } catch (e) {
      await this.query('rollback')
      console.error(e.message)
    }
  }


  /**
   *
   * @param mutations
   */
  async test(mutations: Mutation[]) {
    console.log(`\n--- now testing mutations---\n`)
    var errored = false
    for (var m of mutations) {
      // We do not try testing on pure leaves.
      // if (m.parents.size > 0 && m.children.size === 0)
        // continue

      try {
        // Whenever we get to this point, we can consider that all local mutations
        // are up. As such, we want to track the down mutations that were applied
        // to reapply them, and them only.

        // console.log(ch.blueBright.bold(` *** trying to down/up ${m.full_name}`))
        await this.query('savepoint "dmut-testing"')

        // const downed = new MutationSet()
        // await m.down(MutationRunner.down_runner(downed))
        // const already_up = this.local.diff(downed)
        // await m.up(MutationRunner.up_runner(already_up))

      } catch(e) {
        errored = true
      } finally {
        await this.query('rollback to savepoint "dmut-testing"')
      }
    }
    if (errored) throw new Error(`Mutations had errors, bailing.`)
  }

}