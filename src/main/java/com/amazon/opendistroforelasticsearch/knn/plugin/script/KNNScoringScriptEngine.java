package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.util.Map;
import java.util.Set;

public class KNNScoringScriptEngine implements ScriptEngine {

    public static final String NAME = "knn";
    private static final String SCRIPT_SOURCE = "knn_score";

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public <FactoryType> FactoryType compile(String name, String code, ScriptContext<FactoryType> context, Map<String, String> params) {
        if (context.equals(ScoreScript.CONTEXT) == false) {
            throw new IllegalArgumentException(getType() + " KNN Vector scoring scripts cannot be used for context [" + context.name + "]");
        }
        // we use the script "source" as the script identifier
        if (!SCRIPT_SOURCE.equals(code)) {
            throw new IllegalArgumentException("Unknown script name " + code);
        }
        ScoreScript.Factory factory = VectorScoreScript.VectorScoreScriptFactory::new;
        return context.factoryClazz.cast(factory);
    }

    @Override
    public Set<ScriptContext<?>> getSupportedContexts() {
        return null;
    }
}
