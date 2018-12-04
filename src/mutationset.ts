
import {Mutation} from './mutation'

export const schema = `"dmut"`
export const table = `"mutations"`
export const tbl = `${schema}.${table}`


/**
 * Dmut mutations are always the first, since they create the table the mutations
 * will be stored in.
 */
export const dmut_mutation = new Mutation().setStatic()
.name `dmut installation`
.auto `create schema ${schema}`
.auto `create table ${tbl} (
  "hash" text primary key,
  "identifier" text,
  "statements" text[],
  "undo" text[],
  "parents" text[],
  "static" boolean,
  "date_applied" Timestamp default now()
)`
.comment `on schema ${schema} is 'The schema holding informations about mutations.'`
.comment `on column ${tbl}."hash" is 'A unique hash identifying the mutation'`
.comment `on column ${tbl}."statements" is 'The list of statements that were applied in this mutation'`
.comment `on column ${tbl}."undo" is 'The statements that would be run if the mutation was abandoned'`
.comment `on column ${tbl}."parents" is 'The list of hashes of mutations that this one depends on'`
.comment `on column ${tbl}."static" is 'Wether this mutation is static, as in it must not be undoed'`
.comment `on column ${tbl}."date_applied" is 'The timestamp at which this mutation was applied to the database'`


/**
 * Holder of local mutations
 */
export class MutationRegistry<M extends Mutation = Mutation> {


  constructor(public mutations = [] as M[], public ctor = Mutation as new () => M) {
    if (mutations.length === 0)
      mutations.push(dmut_mutation as M)
  }

  protected _add(m: M) {
    this.mutations.push(m)
    return m
  }

  depends(...ms: Mutation[]) {
    return this._add(new this.ctor()
      .depends(...ms))
  }

  get create() {
    return this._add(new this.ctor())
  }

  get static() {
    return this._add(new this.ctor()
      .setStatic())
  }

  /**
   * Clones the mutationset, excluding the given hashes
   * @param hash the hash to exclude
   */
  without(hashes: string[]) {
    const h = new Set(hashes)
    const newmuts = [] as Mutation[]

    function tag(m: Mutation) {
      h.add(m.hash)
      for (var c of m.children)
        tag(c)
    }

    for (var m of this.mutations) {
      if (h.has(m.hash))
        tag(m)
    }

    for (var m of this.mutations) {
      // Now that they're all tagged, push the mutations that are
      // still included
      if (!h.has(m.hash))
        newmuts.push(m)
    }
    return new MutationRegistry(newmuts)
  }

  [Symbol.iterator]() {
    return this.mutations[Symbol.iterator]()
  }

}