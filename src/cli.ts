import * as fs from 'fs'
import * as pg from 'pg'
import { Mutation, DmutBaseMutation, DmutComments } from './mutation'
import { MutationRunner } from './database'
// import * as util from 'util'
import { Seq, R, Either, Parser, NoMatch, Res, escape } from 'parseur'

const args = process.argv.slice(3)
const files = new Map<string, string>()


const ID = R(/[$\w_]+/).map(r => r[0])

const SQLID_BASE = R(/"(""|[^"])*"|[@$\w_]+|\[[^\]]+\]|`(``|[^`])*`/).map(r => r[0])
const SQLID = Seq({
  id:     SQLID_BASE,
  rest:   Seq({dot: '.', id: SQLID_BASE }).map(r => r.dot + r.id).Repeat()
}).map(r => [r.id, ...r.rest].join(''))// .tap(((c, inp, pos) => console.log(c, pos)))

const down = <T>(fn: (r: T) => string) => (r: T, input: string, pos: number, skip: RegExp | undefined, start: number) => [
  { kind: 'down', contents: fn(r) },
  { kind: 'up', contents: input.slice(start, pos).trim() }
]

const S = (tpl: TemplateStringsArray) => R(new RegExp(tpl[0].replace(/\s+/g, '\\s+'), 'i'))

// The easily droppable ones, where undoing them is just about dropping their ID.
const R_Autos = Seq(
              S`create`,
    {
      type:   S`table|index|role|extension|schema|type|(materialized\s+)?view`.map(r => r[0]),
      id:     SQLID
    },
              /[^;]*;/ // this should probably be different.
  ).map(down(r => `drop ${r.type} ${r.id};`))

const R_Auto_Grant = Seq(
                S`grant`,
    { rights:   R(/((?!on)[^])+/i).map(r => r[0]) },
                S`on`,
    { obj:      SQLID },
                S`to`,
    { to:       SQLID },
                /[^;]*;/ // this should probably be different.
  ).map(down(r => `revoke ${r.rights} on ${r.obj} from ${r.to};`))


const R_Auto_Trigger = Seq(
                S`create`,
    { kind:     S`policy|trigger` },
    { trigger:  SQLID },
                /((?!on)[^])*on/i,
    { table:    SQLID },
                /[^;]*;/
  ).map(down(r => `drop ${r.kind} ${r.trigger} on ${r.table}`))


const R_Auto_Function = Seq(
            S`create (or replace )?function`,
  {
    name:   SQLID,
    args:   /\([^\)]*\)/
  },
            /((?!as)(.|\n))*as\s+/i, // we go until the function definition.
            Either(
              R(/[$_\w]+/).map(r => {
                const reg = `(?:(?!${escape(r[0])})[^])*?${escape(r[0])}`
                // console.log(reg)
                return R(new RegExp(reg, 'i'))
              }),
              /'(''|[^])*'/,
            ),
            /[^;]*;/
            // /\$\$((?!\$\$)(?:\n|.))*\$\$[^;]*;/i,
).map(down(r => `drop function ${r.name}${r.args};`))


const R_Auto_RLS = Seq(
        S`alter table`,
        { table: SQLID },
        S`enable row level security\\s*;`
).map(down(r => `alter table ${r.table} disable row level security;`))


const R_Auto_Comment = R(/comment[^;]+;/i).map(r => [ { kind: 'up', contents: r[0] } ])


const RMutation = Seq(
                /mutation/i,
    { id:       ID,
    depends:    Seq(
                              /depends\s+on/i,
                  { depends:  ID.SeparatedBy(',') }
                ).map(r => r.depends).Optional(),
    search:     Seq(
                          /with\s+search\s+path/i,
                  { sp:   ID.SeparatedBy(',') }
                ).map(r => r.sp).Optional(),
    statements: Either(R_Autos, R_Auto_Grant, R_Auto_Trigger, R_Auto_Function, R_Auto_RLS, R_Auto_Comment).Repeat()
  })

const Mutations = RMutation.Repeat()

var fparse = Parser(Mutations, /([\n\s \t\r]|--[^\n]*\n?)+/)


// A mutation map, with their statements
const mutations = new Map<string, {mutation: Mutation, parents: string[]}>()

for (var arg of args) {
  const cts = fs.readFileSync(arg, 'utf-8')
  files.set(arg, cts)
  const res = fparse(cts)
  if (res === NoMatch) {
    console.log(arg, 'did not match')
    if (Res.max_res) {
      // console.log(Res.max_res)
      const line = cts.slice(0, Res.max_res.pos).split(/\n/g).length
      console.log(`Input left line ${line}: "${cts.slice(Res.max_res.pos, Res.max_res.pos + 100)}..."`)
    }
  } else {
    var presult = res.res
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
  const parents = m.parents?.map(p => {
    const res = mutations.get(p)
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

// const all_mutations = [...mutations.values()].map(m => m.mutation)
const client = new pg.Client(process.argv[2])
client.connect().then(() => {
  const runner = new MutationRunner(client)
  return runner.mutate(all_mutations)
}).then(e => {
  console.log('done.')
  client.end()
}).catch(e => console.error(e))
// console.log(files)