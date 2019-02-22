
import * as cr from 'crypto'

/**
 * Memoize un appel d'un get(). À n'utiliser que sur des properties calculées,
 * pas sur des propriétés d'une classe.
 *
 * @param target The class instance
 * @param key The property name
 * @param descriptor The property descriptor
 */
export function memoize(target: any, key: string, descriptor: PropertyDescriptor) {
	const sym = Symbol(key)
	const orig = descriptor.get!

	descriptor.get = function (this: Object) {
		if (!this.hasOwnProperty(sym)) {
			Object.defineProperty(this, sym, {value: orig.call(this), enumerable: false})
		}
		return (this as any)[sym]
	}

}

//////////////////////////////////////////////////////////////////////////
// Some regular expression helpers, to write some nicer regexps.

function sep_by(pattern: RegExp, sep: RegExp) {
  return new RegExp(`(?:(?:${pattern.source})${sep.source})*(?:${pattern.source})`)
}

const patterns: {[name: string]: RegExp} = {
  id: sep_by(/[@\w]+|"[^"]+"|`[^`]+`|\[[^\]]\]/, /\s*\.\s*/),
  create: /create\s*(?:\s+\w+)*?/
}

function mkregex(reg: RegExp) {
  var src = reg.source
  for (var x in patterns)
    src = src.replace(new RegExp(`:${x}`, 'g'), patterns[x].source)
  src = src.replace(/(\s|\n)/g, ' ').replace(/ +/g, /(?:\s|\n|\r|\t)+/.source)
  return new RegExp('^\\s*' + src, 'i')
}


export const auto_makers = new Map<RegExp, ((...a: string[]) => string)>()

auto_makers.set(
  mkregex(/:create (role|table|type|extension|schema|(?:materialized\s+)?view) (:id)/),
  (type, id) => {
    return `drop ${type} ${id}`
  }
)

auto_makers.set(
  mkregex(/:create index (:id) ON (:id)/),
  (idx_name, on) => {
    if (on.includes('.')) {
      idx_name = on.split('.')[0] + '.' + idx_name
    }
    return `drop index ${idx_name}`
  }
)

auto_makers.set(
  mkregex(/grant ([^]+) on ((?:\w+ )?:id) to (:id)/),
  (rights, what, to) => {
    return `revoke ${rights} on ${what} from ${to}`
  }
)

auto_makers.set(
  mkregex(/:create function (:id)\s*\(([^\)]*)\)/),
  (name, args) => {
    return `drop function ${name}(${args})`
  }
)

auto_makers.set(
  mkregex(/:create trigger (:id) (?:(?! on)|[^])+ on (:id)/),
  (name, table) => {
    return `drop trigger ${name} on ${table}`
  }
)

auto_makers.set(
  mkregex(/alter table (:id) enable row level security/),
  (table) => {
    return `alter table ${table} disable row level security`
  }
)

auto_makers.set(
  mkregex(/create policy (\w+) on (:id)/),
  (name, table) => {
    return `drop policy ${name} on ${table}`
  }
)

export function tpljoin(s: TemplateStringsArray, a: any[]) {
  const res = [] as string[]
  // console.log(s, a)
  for (var i = 0; i < s.length - 1; i++) {
    res.push(s[i])
    res.push(a[i].toString())
  }
  res.push(s[i])
  return res.join('').trim()
}


export class Mutation {

  identifier: string = ''
  children: Mutation[] = []
  parents: Mutation[] = []
  statements: string[] = []
  undo: string[] = []
  static: boolean = false

  setStatic() {
    this.static = true
    return this
  }

  @memoize
  get hash(): string {
    const hash = cr.createHash('sha1') // this should be enough to avoid collisions

    // we have to be smart about the source and remove only the parts we don't want
    // to compare only the code.
    for (var s of [...this.statements, ...this.undo]) {
      const replaced = s
        // We remove single line comments, except if they start with a !, as it has meaning to us.
        .replace(/--(?!\s*!).*?$/gm, '')
        // we are not handling recursive comments, and we don't care.
        .replace(/\/\*((?!\*\/)(.|\r|\n))*?\*\//mg, '')
        // whitespace should not affect if our file changed or not.
        .replace(/[\n\r\t\s]/g, ' ')
        .replace(/ +/g, ' ')
      hash.update(replaced)
    }

    for (var p of this.parents) {
      hash.update(p.hash)
    }

    return hash.digest('hex')
  }

  depends(...ms: Mutation[]) {
    // Add parents and children.
    for (var m of ms) {
      if (this.static && !m.static)
        throw new Error(`A static mutation can't depend on a non-static one.`)

      m.children.push(this)
      this.parents.push(m)
    }

    return this
  }

  name(str: TemplateStringsArray, ...a: any[]) {
    this.identifier = tpljoin(str, a)
    return this
  }

  auto(str: TemplateStringsArray, ...a: any[]) {
    const stmt = tpljoin(str, a)

    for (var [re, action] of auto_makers.entries()) {
      const match = re.exec(stmt)
      if (match == null)
        continue
      const args = match.slice(1)
      const result = action(...args) + ' -- @auto'
      this.undo.unshift(result)
      // console.log(result)
      this.statements.push(stmt)
      return this

    }
    throw new Error(`Unrecognized statement for auto(): ${stmt}`)
  }

  protected up(str: TemplateStringsArray, ...a: any[]) {
    const stmt = tpljoin(str, a)
    this.statements.push(stmt)
    // Devrait renvoyer down.
    return this as this
  }

  comment(str: TemplateStringsArray, ...a: any[]) {
    this.statements.push('comment ' + tpljoin(str, a))
    return this as this
  }

  down<M extends this>(str: TemplateStringsArray, ...a: any[]) {
    const stmt = tpljoin(str, a)
    this.undo.unshift(stmt)
    const _t = this
    return {
      up(str: TemplateStringsArray, ...a: any[]): M {
        _t.up(str, ...a)
        return _t as M
      }
    }
  }

  fn(name: string, fn: Function, ...types: string[]) {

    const re_js = /^\s*function(?=\s+\w+)?\s*\(([^\)]*)\)\s*\{([^]*)\}\s*/igm

    const src = fn.toString()
    // console.log(src)
    const match = re_js.exec(src)
    if (!match) throw new Error(`Unable to parse function !`)

    const args = match[1] ? match[1]
      .split(',').map((a, i) => `${a.trim()} ${types[i]}`.replace(/...(\w+) (\w+)/g, (all, arg, type) => {
      return `variadic ${arg} ${type}[]`
    })) : []

    if (args.length !== types.length - 1)
      throw new Error(`You must define the same number of types for your function arguments as well as the return type.`)
    const body = match[2].trim()

    const stmt = `create function ${name}(${args}) returns ${types[types.length - 1]} as $$
      ${body}
    $$ language plv8`
    const undo = `drop function ${name}(${args})`
    this.statements.push(stmt)
    this.undo.unshift(undo)
    return this
  }

}
