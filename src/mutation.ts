
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


export const re_auto = /^\s*create\s+(\w+)\s+((?:(?:\w+|"[^"]+"|`[^`]+`|\[[^\]]\])\.)*(?:\w+|"[^"]+"|`[^`]+`|\[[^\]]\]))/i


export function tpljoin(s: TemplateStringsArray, a: any[]) {
  const res = [] as string[]
  for (var i = 0; i < s.length - 1; i++) {
    res.push(s[i])
    res.push(a[i].toString())
  }
  res.push(s[i])
  return res.join('').trim()
}


export class Mutation {
  deps = new Set<Mutation>()
  children = new Set<Mutation>()
  statements: string[] = []
  undo: string[] = []
  static: boolean = false

  static depends(...ms: Mutation[]) {
    return new Mutation()
      .depends(...ms)
  }

  static get create() {
    return new Mutation()
  }

  static get static() {
    return new Mutation()
      .setStatic()
  }

  setStatic() {
    this.static = true
    return this
  }

  @memoize
  get hash(): string {
    const hash = cr.createHash('sha256') // this should be enough to avoid collisions

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

    return hash.digest('hex')
  }

  depends(...ms: Mutation[]) {
    // Add parents and children.
    for (var m of ms) {
      if (this.static && !m.static)
        throw new Error(`A static mutation can't depend on a non-static one.`)

      m.children.add(this)
      this.deps.add(this)
    }

    return this
  }

  auto(str: TemplateStringsArray, ...a: any[]) {
    const stmt = tpljoin(str, a)
    const match = re_auto.exec(stmt)
    if (match == null)
      throw new Error(`Unrecognized statement for auto(): ${stmt}`)
    const type = match[1]
    const ident = match[2]
    this.undo.push(`drop ${type} ${ident}`)
    console.log(`drop ${type} ${ident} -- auto-generated`)
    this.statements.push(stmt)
    return this
  }

  protected up(str: TemplateStringsArray, ...a: any[]) {
    const stmt = tpljoin(str, a)
    this.statements.push(stmt)
    // Devrait renvoyer down.
    return this
  }

  down(str: TemplateStringsArray, ...a: any[]) {
    const stmt = tpljoin(str, a)
    this.undo.unshift(stmt)
    const _t = this
    return {
      up(str: TemplateStringsArray, ...a: any[]): Mutation {
        _t.up(str, ...a)
        return _t
      }
    }
  }

  fn(fn: Function, ...types: string[]) {

    if (fn.length !== types.length - 1)
      throw new Error(`You muse define the same number of types for your function arguments as well as the return type.`)

    const re_js = /^\s*function\s+\w+\s*\(([^\)]*)\)\s*\{([^]*)\}\s*/igm

    const src = fn.toString()
    const match = re_js.exec(src)
    if (!match) throw new Error(`Unable to parse function !`)

    const args = match[1].split(',').map((a, i) => `${a.trim()} ${types[i]}`.replace('...', 'variadic '))
    console.log(args)
    const body = match[2].trim()

    const stmt = `create function ${fn.name}(${args}) returns ${types[types.length - 1]} as $$
      ${body}
    $$ language plv8`
    console.log(stmt)
    const undo = `drop function ${fn.name}(${types.join(',')})`
    this.statements.push(stmt)
    this.undo.unshift(undo)
  }

}
