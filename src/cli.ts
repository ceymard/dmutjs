#!/usr/bin/node --enable-source-maps
import * as fs from 'fs'
import * as pg from 'pg'
import { Mutation, DmutBaseMutation, DmutComments } from './mutation'
import { MutationRunner } from './database'
import ch from 'chalk'
// import * as util from 'util'

import { Seq, Either, NoMatch, AnyTokenUntil, Parseur, Repeat, Context, Not, Rule, AnyTokenBut, SeparatedBy, Opt } from 'parseur'

const args = process.argv.slice(3)
const files = new Map<string, string>()


export class DmutContext extends Context {
  current_marker?: string
}

var P!: DmutParser['P']
var A!: DmutParser['A']
export class DmutParser extends Parseur<DmutContext> {

  A(tpl: TemplateStringsArray) {
    var strs = tpl[0].split(/\s+/g).map(str => this.SqlId.then(i => {
      if (i.toLowerCase() !== str) return NoMatch
      return i
    }))
    return strs.length === 1 ? strs[0] : Seq(...strs).then(s => tpl[0])
  }

  /* @ts-ignore */
  private _1 = P = this.P
  private _2 = A = this.A.bind(this)

  // leftover tokens
  // ... unused for now but could be used ?
  leftovers = [
    '::', ':', '(', ')', '[', ']', '{', '}', '|', '=', '#', '?', '!', '~', '-', '>', '<', '+', '*', '/', '|', '%',
  ].map(t => this.token(t))
  NUM = this.token(/\d+/)
  //

  // ID = this.token(/[$a-zA-Z_]\w+/)
  SQLID_BASE = this.token(/"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\]|`(``|[^`])*`/)
  STRING = this.token(/'(''|[^'])*'/)
  WS = this.token(/(\s|--[^\n]*\n?)+/).skip()

  // Id = this.ID.then(i => i.str)

  SqlIdBase = this.SQLID_BASE.then(s => s.str)

  SqlId = Seq({
    id:     this.SqlIdBase,
    rest:   Repeat(P`. ${this.SqlIdBase}`.then(r => '.' + r))
  }).then(r => [r.id, ...r.rest].join(''))

  Until = <R extends Rule<any, any>>(rule: R) => Seq(Repeat(Not(rule)), rule)

  // The easily droppable ones, where undoing them is just about dropping their ID.
  R_Autos = Seq(
    A`create`,
    { type:   Either(
                Either(
                  A`table`,
                  A`role`,
                  A`extension`,
                  A`schema`,
                  A`type`,
                  A`view`,
                  A`materialized view`
                ),
              ) },
    { id:     this.SqlId },
    AnyTokenUntil(P`;`),
  ).then(down(r => `drop ${r.type} ${r.id};`))

  R_Auto_Index = Seq(
    A`create`,
    Opt(A`unique`),
    A`index`,
    { idx: this.SqlId },
    A`on`,
    { tbl: this.SqlId },
    AnyTokenUntil(P`;`)
  ).then(down(r => `drop index ${r.tbl.includes('.') ? r.tbl.split('.')[0] + '.': ''}${r.idx}`))

  R_Auto_Grant = Seq(
        A`grant`,
    { rights:   AnyTokenUntil(A`on`)
            .then(r => r.tokens.map(t => t.str).join(' ')) },
    { obj:      this.SqlId },
        A`to`,
    { to:       this.SqlId },
        AnyTokenUntil(P`;`),
  ).then(down(r => `revoke ${r.rights} on ${r.obj} from ${r.to};`))


  R_Auto_Trigger = Seq(
      A`create`,
  { kind:     Either(A`policy`, A`trigger`) },
  { trigger:  this.SqlId },
      AnyTokenUntil(A`on`),
  { table:    this.SqlId },
      AnyTokenUntil(P`;`),
  ).then(down(r => `drop ${r.kind} ${r.trigger} on ${r.table}`))


  R_Auto_Function = Seq(
    A`create`, Opt(A`or replace`), A`function`,
    {
      name:   this.SqlId,
      args:   AnyTokenUntil(P`)`, { include_end: true }).then(r => r.tokens.map(t => t.str).join(''))
      // args:   /\([^\)]*\)/
    },
    AnyTokenUntil(A`as`),
    // /((?!as)(.|\n))*as\s+/i, // we go until the function definition.
    Either(
      // Match a named string
      Seq(
        this.SqlIdBase.then((i, ctx) => {
          ctx.current_marker = i
          return i
        }),
        AnyTokenUntil(this.SqlIdBase.then((i, ctx) => i !== ctx.current_marker ? NoMatch : i)),
      ),
      // Or just a regular string.
      this.STRING,
    ),
    AnyTokenUntil(P`;`),
  ).then(down(r => `drop function ${r.name}${r.args};`))


  R_Auto_RLS = Seq(
    A`alter table`,
    { table: this.SqlId },
    A`enable row level security`, P`;`
  ).then(down(r => `alter table ${r.table} disable row level security;`))

  R_Auto_Comment = Seq(A`comment`, AnyTokenBut(P`;`), P`;`).then(r => [ { kind: 'up', contents: r[0] } ])

  RMutation = Seq(
      A`mutation`,
  { id:       this.SqlId,
  depends:    Opt(Seq(A`depends on`,
        { depends:  SeparatedBy(P`,`, this.SqlId) }
      ).then(r => r.depends)),
  search:     Opt(Seq(
        A`with search path`,
        { sp:   SeparatedBy(P`,`, this.SqlId) }
      ).then(r => r.sp)),
  statements: Repeat(Either(
    this.R_Autos,
    this.R_Auto_Index,
    this.R_Auto_Grant,
    this.R_Auto_Trigger,
    this.R_Auto_Function,
    this.R_Auto_RLS,
    this.R_Auto_Comment))
  })

  Mutations = Seq({ res: Repeat(this.RMutation) }, this.Eof).then(r => r.res)

  parse(input: string) {
    return this.parseRule(input, this.Mutations, tk => new DmutContext(tk), { enable_line_counts: true })
  }

}


const down = <T>(fn: (r: T) => string) => (r: T, ctx: Context, pos: number, start: number) => [
  { kind: 'down', contents: fn(r) },
  { kind: 'up', contents: ctx.input.slice(start, pos).map(c => c.str).join('').trim() }
]


const parser = new DmutParser()
parser.nameRules()
// A mutation map, with their statements
const mutations = new Map<string, {mutation: Mutation, parents: string[]}>()

for (var arg of args) {
  var cts = fs.readFileSync(arg, 'utf-8')
  files.set(arg, cts)
  var res = parser.parse(cts)
  if (res.status === 'tokenerror') {
    console.log(arg, 'did not lex', res.max_pos, ch.grey(`...'${cts.slice(res.max_pos, res.max_pos + 100)}'`))
  } else if (res.status === 'nomatch') {
    // console.log(res.rule)
    // var tk = res.tokens[res.pos]
    console.log(arg, `did not match`, res.pos,  res.tokens.slice(res.pos, res.pos + 5).map(t => `T<${t.str}:${t.def._name}@${t.line}>`))
    throw new Error(`Parse failed`)
  } else {
    var presult = res.value
    for (let mp of presult) {
      // console.log(mp.id)
      var m = new Mutation(mp.id)
      if (mp.search) {
        m.statements.unshift(`set search_path = ${mp.search.join(', ')};`)
        m.statements.push(`reset search_path;`)
        m.undo.unshift(`set search_path = ${mp.search.join(', ')};`)
        m.undo.push(`reset search_path;`)
        // m.searchPath(mp.search)
      }
      mutations.set(mp.id, { mutation: m, parents: mp.depends ?? [] })
      for (var stmts of mp.statements) {
        for (var stmt of stmts) {
          if (stmt.kind === 'down') m.down(stmt.contents)
          else if (stmt.kind === 'up') m.up(stmt.contents)
        }
      }
    }
  }
  // console.log(arg, util.inspect(fparse(cts), undefined, null, true))

  // var lexed = lexer.feed(cts)
  // console.log(lexed.map(l => l.str))

  // We now have to break up individual mutations...
}

// console.log(mutations.keys())
for (let m of mutations.values()) {
  // console.log(m)
  var parents = m.parents?.map(p => {
    var res = mutations.get(p)
    if (!res) throw new Error(`Mutation "${m.mutation.identifier}" depends on inexistant mutation "${p}"`)
    return res.mutation
  })
  if (parents)
    m.mutation.depends(parents)
}


const all_mutations = new Set<Mutation>()
all_mutations.add(DmutBaseMutation).add(DmutComments)
for (let m of mutations.values()) {
  all_mutations.add(m.mutation)
}

// console.log(all_mutations)
// const all_mutations = [...mutations.values()].map(m => m.mutation)
// process.exit(0)
const client = new pg.Client(process.argv[2])
client.connect().then(() => {
  var runner = new MutationRunner(client)
  return runner.mutate(all_mutations)
}).then(e => {
  console.log('done.')
  client.end()
}).catch(e => console.error(e))
// console.log(files)