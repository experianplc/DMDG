declare module 'tailored'  {
  class Wildcard {
  }

  class Variable {
    name: string;
    default_value: any;
  }

  class Clause {
    pattern: boolean;
    arity: number;
    options?: any[];
    fn: Function;
    guard: Function;
  }

  function wildcard(): Wildcard;
  function variable(name?: string, default_value?: any): Variable;
  function defmatch(...clauses: any[]): ((args: any) => any)
  function clause(pattern: any, fn: Function, guard?: Function): Clause
}
