package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.util.Map;
import java.util.Set;

/**
 * KNN Custom scoring Engine implementation.
 */
public class KNNScoringScriptEngine implements ScriptEngine {

    public static final String NAME = "knn";
    public static final String SCRIPT_SOURCE = "knn_score";

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public <FactoryType> FactoryType compile(String name, String code, ScriptContext<FactoryType> context, Map<String, String> params) {
        KNNCounter.SCRIPT_COMPILATIONS.increment();
        if (!ScoreScript.CONTEXT.equals(context)) {
            KNNCounter.SCRIPT_COMPILATION_ERRORS.increment();
            throw new IllegalArgumentException(getType() + " KNN Vector scoring scripts cannot be used for context [" + context.name + "]");
        }
        // we use the script "source" as the script identifier
        if (!SCRIPT_SOURCE.equals(code)) {
            KNNCounter.SCRIPT_COMPILATION_ERRORS.increment();
            throw new IllegalArgumentException("Unknown script name " + code);
        }
        ScoreScript.Factory factory = KNNVectorScoreScript.VectorScoreScriptFactory::new;
        return context.factoryClazz.cast(factory);
    }

    @Override
    public Set<ScriptContext<?>> getSupportedContexts() {
        return null;
    }
}
