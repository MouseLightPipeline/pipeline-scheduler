import {makeExecutableSchema, addMockFunctionsToSchema} from "graphql-tools";

import typeDefinitions from "../pipelineTypeDefinitions";
import resolvers from "../pipelineServerResolvers";

let executableSchema = makeExecutableSchema({
    typeDefs: typeDefinitions,
    resolvers: resolvers,
    resolverValidationOptions: {
        requireResolversForNonScalar: false
    }
});

/*
addMockFunctionsToSchema({
    schema: executableSchema,
    mocks: {
        String: () => "Not implemented",
        DateTime: () => Date.now()
    },
    preserveResolvers: true
});
*/

export {executableSchema as schema};
