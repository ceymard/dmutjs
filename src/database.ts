
import {Client, QueryResult} from 'pg'
import {Mutation, DmutBaseMutation} from './mutation'

const tbl = `"dmut"."mutations"`


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
  parents: string[]
  date_applied: Date

  // True if this mutation should be kept
  keep?: boolean
}


export class MutationRunner {

  testing = false

  constructor(
    public client: Client,
    // public base_mutation: Mutation
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
      const pos = e.position - 1
      var re = /[\w$."']+|[^\w\s]+/y
      re.lastIndex = pos
      var m = re.exec(stmt)
      var str = ch.grey(stmt.slice(0, pos)) + ch.red(stmt.slice(pos))
      if (m) {
        str = ch.grey(stmt.slice(0, pos)) + ch.red(m[0]) + ch.grey(stmt.slice(pos + m[0].length))
      }
      console.log(`${ch.grey.bold('On statement:')}\n  ${str}`)
      console.log(`  ${ch.redBright(e.message)}`)
      throw e
    }
  }

  /**
   * Fetch the hashes of the mutations we have in database.
   */
  async fetchRemoteMutations(): Promise<MutationRow[]> {
    try {
      // just check if table exists pretty quickly. This will fail if the table does not exist
      await this.client.query(`select 1 from ${tbl} limit 1`)
    } catch {
      return []
    }

    try {
      const res = await this.client.query(`select * from ${tbl}`)
      const rows = res.rows as MutationRow[]
      const result = [] as MutationRow[]
      const dct = rows.reduce((acc, item) => {
        acc[item.hash] = item
        return acc
      }, {} as {[hash: string]: MutationRow})

      const seen = new Set<string>()

      const add = (m: MutationRow) => {
        if (seen.has(m.hash)) return

        for (var c of m.parents)
          add(dct[c])

        seen.add(m.hash)
        result.push(m)
      }

      for (var m of res.rows as MutationRow[]) {
        add(m)
      }
      // console.log(result.map(m => [m.identifier, m.hash]))
      return result as MutationRow[]
    } catch (e) {
      console.log(e.message)
      throw e
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
  async mutate(_mutations: Set<Mutation>, test = false) {

    // Add the mutations in the order they need to run.
    function _add(m: Mutation) {
      if (mutations.has(m)) return // already handled.
      for (var p of m.parents) {
        _add(p)
      }
      mutations.add(m)
    }
    var mutations = new Set<Mutation>()
    for (let _m of _mutations) _add(_m)

    const dbmut = await this.fetchRemoteMutations()
    // const dbdct = this.mkdct(dbmut)
    const local_dct = {} as {[hash: string]: Mutation}
    for (let mut of mutations) {
      local_dct[mut.hash] = mut
      if (mut.hash_lock && !mut.hash.startsWith(mut.hash_lock))
        throw new Error(`Mutation locked on ${mut.hash_lock} | ${mut.identifier} is locked but has changed (current hash is ${mut.hash}), cannot proceed`)
    }

    // These mutations will have to go
    const gone = [] as MutationRow[]

    // But these will stay.
    const staying = [] as MutationRow[]
    const local_staying = [] as Mutation[]

    for (var d of dbmut) {
      if (!local_dct[d.hash]) {
        gone.push(d)
      } else {
        staying.push(d)
        local_staying.push(local_dct[d.hash])
      }
    }
    const output = [] as string[]

    local_staying.sort((a, b) => a.identifier < b.identifier ? -1 : a.identifier > b.identifier ? 1 : 0)
    for (let stay of local_staying)
      output.push(`  ${stay.hash_lock ? '♖' : '≋'} ${ch.gray(stay.hash.slice(0, 8))} ${ch.yellow(stay.identifier || stay.hash)}`)

    // We have to de-apply mutations in reverse order
    gone.reverse()

    // This will be used to avoid upping a local mutation.
    // const still_there = this.mkdct(staying)
    const still_there = new Set<string>(staying.map(m => m.hash.trim()))
    var touched = false
    if (!this.testing) await this.query('begin')
    try {

      for (var rm of gone) {
        // if (rm.static)
        //   throw new Error(`cannot undo a static mutation, yet ${rm.hash} is no longer here`)

        // LOG that we're destroying a mutation ?
        touched = true
        output.push(`  « ${ch.gray(rm.hash.slice(0, 8))} ${ch.redBright(rm.identifier || rm.hash)}`)

        for (var undo of rm.undo) {
          await this.query(undo)
        }

        await this.query(/* sql */`delete from ${tbl} where hash = $1`, [rm.hash])
      }

      // Local is always in the good order, since children cannot be declared
      // before their parents.
      const to_apply = mutations
      for (var t of to_apply) {
        if (still_there.has(t.hash)) continue
        output.push(`  » ${ch.gray(t.hash.slice(0, 8))} ${ch.greenBright(t.identifier || t.hash)}${t.parents.size ? ' :: ' + ch.gray([...t.parents].map(p => p.identifier).join(' | ')) : ''}`)
        touched = true

        for (var stmt of t.statements) {
          // console.log(stmt)
          await this.query(stmt)
        }

        await this.query(`insert into ${tbl}(identifier, hash, statements, undo, parents, namespace)
          values($1, $2, $3, $4, $5, $6)`,
          [t.identifier, t.hash, t.statements, t.undo, Array.from(t.parents).map(c => c.hash), null]
        )
      }

      // Once we're done, we might want to commit...
      // await query('rollback')
      if (!this.testing && touched) {
        if (test) {
          console.log(`now testing mutations`)
          await this.test(mutations)
        }
        await this.query('commit')
      }

      if (!this.testing) {
        output.forEach(o => console.log(o))
      }

      if (!touched)
        await this.query('rollback')

    } catch (e) {
      if (!this.testing) {
        // console.log(`Rolling back all of it since we have an error`)
        await this.query('rollback')
      }

      output.forEach(o => console.log(o))
      // console.log(e.message)
      throw e
    }
  }

  /**
   *
   * @param base_mutation
   */
  async test(mutations: Set<Mutation>) {
    // At this point, we already mutated all of our local mutations.
    // We will now try to remove them one by one and see if they hold
    this.testing = true

    // console.log(`\n--- now testing mutations---\n`)
    var errored = false
    for (var m of mutations) {
      // Not testing the basic dmut mutation
      if (m.hash === DmutBaseMutation.hash) continue

      try {
        // Whenever we get to this point, we can consider that all local mutations
        // are up. As such, we want to track the down mutations that were applied
        // to reapply them, and them only.

        // console.log(ch.greenBright(`  ==> Testing removal of ${m.identifier || m.hash}`))
        await this.query('savepoint "dmut-testing"')

        // Try removing this mutation from our local list
        const local = Mutation.mutationsWithout(mutations, new Set([m]))

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
