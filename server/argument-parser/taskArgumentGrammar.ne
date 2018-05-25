@builtin "whitespace.ne" # `_` means arbitrary amount of whitespace

@{%
const moo = require("moo");

const lexer = moo.compile({
  ws:     /[ \t]+/,
  word: /[^\s}{]+/, // /[a-zA-Z0-9=_\-\[\]\(\)\/"]+/,
  times:  /\*|x/,
  '{': '{',
  '}': '}',
   esc: '\\ '
});

const isLogging = false;

const LITERAL = 0;
const PARAMETER = 1;

const log_inputs = (d, index) => {
    if (!isLogging) return;

    console.log(index);
    console.log(d);
}

const combineElements = (index) => {
    return (d) => {
        log_inputs(d, index);

        if (d[1].length === 0) {
            return [d[0].value];
        }

        return [d[0].value + " " + d[1].map(e => e[1].value).join(" ")];
    }
}

const combineEntries = (index) => {
    return (d) => {
        log_inputs(d, index);

        if (d[1].length === 0) {
            return [d[0]];
        }

        return [d[0]].concat(d[1].map(e => e[1]));
    }
}
%}

# Pass your lexer object using the @lexer option:
@lexer lexer

arg_list -> arg (_ arg):*  {% combineEntries(1) %}

arg -> parameter_arg {% (d) => { return {value: d[0][0], type: PARAMETER}} %}
        | literal_arg {% (d) => { return {value: d[0][0], type: LITERAL}} %}

parameter_arg ->  "{" literal_arg "}" {% (d) => {return d[1];} %}

literal_arg -> %word (%esc %word):* _  {% combineElements(0) %}

# simple -> %word {% (d) => {return d[0].value;} %}
