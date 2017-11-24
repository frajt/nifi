/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.attribute.expression.language;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import org.apache.nifi.attribute.expression.language.compile.CompiledExpression;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.AllAttributesEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.AnyAttributeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.AttributeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.MappingEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.MultiAttributeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.MultiMatchAttributeEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.selection.MultiNamedAttributeEvaluator;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.processor.exception.ProcessException;

public class StandardPreparedQuery implements PreparedQuery {

    private final List<String> queryStrings;
    private final Map<String, ExpressionEntry> expressionEntries;
    private volatile VariableImpact variableImpact;
    
    private static final class ExpressionEntry {
      private final CompiledExpression compiledExpression;
      private final TransferQueue<Query> queryPool = new LinkedTransferQueue<>();
      
      public ExpressionEntry(final CompiledExpression compiledExpression) {
        this.compiledExpression = compiledExpression;        
      }      
    }
    

    public StandardPreparedQuery(final List<String> queryStrings, final Map<String, CompiledExpression> expressions) {
        System.err.println("NEW: StandardPreparedQuery : " + queryStrings);
        this.queryStrings = queryStrings;
        this.expressionEntries = new HashMap<>(expressions.size());
        expressions.forEach((val, compiledExpression) -> {
          this.expressionEntries.put(val, new ExpressionEntry(compiledExpression));
        });        
    }

    @Override
    public String evaluateExpressions(final Map<String, String> valMap, final AttributeValueDecorator decorator, final Map<String, String> stateVariables) throws ProcessException {
        final StringBuilder sb = new StringBuilder();
        for (final String val : queryStrings) {
            final ExpressionEntry expressionEntry = expressionEntries.get(val);
            if (expressionEntry == null) {
                sb.append(val);
            } else {              
                // get pooled query
                Query query = expressionEntry.queryPool.poll();
                if(query == null) {
                  // create new query from the tree, we could somehow reuse the root evaluator if a clone/twin would be supported
                  query = Query.fromTree(expressionEntry.compiledExpression.getTree(), val);
                }
                try {
                  final String evaluated = query.evaluateExpression(valMap, decorator, stateVariables);
                  if (evaluated != null) {
                      sb.append(evaluated);
                  }
                } finally {
                  // return query back to the pool
                  expressionEntry.queryPool.add(query);
                }
            }
        }
        return sb.toString();
    }

    @Override
    public String evaluateExpressions(final Map<String, String> valMap, final AttributeValueDecorator decorator)
            throws ProcessException {
        return evaluateExpressions(valMap, decorator, null);
    }

    @Override
    public boolean isExpressionLanguagePresent() {
        return !expressionEntries.isEmpty();
    }

    @Override
    public VariableImpact getVariableImpact() {
        final VariableImpact existing = this.variableImpact;
        if (existing != null) {
            return existing;
        }

        final Set<String> variables = new HashSet<>();

        for (final ExpressionEntry expressionEntry : expressionEntries.values()) {
            for (final Evaluator<?> evaluator : expressionEntry.compiledExpression.getAllEvaluators()) {
                if (evaluator instanceof AttributeEvaluator) {
                    final AttributeEvaluator attributeEval = (AttributeEvaluator) evaluator;
                    final Evaluator<String> nameEval = attributeEval.getNameEvaluator();

                    if (nameEval instanceof StringLiteralEvaluator) {
                        final String referencedVar = nameEval.evaluate(Collections.emptyMap()).getValue();
                        variables.add(referencedVar);
                    }
                } else if (evaluator instanceof AllAttributesEvaluator) {
                    final AllAttributesEvaluator allAttrsEval = (AllAttributesEvaluator) evaluator;
                    final MultiAttributeEvaluator iteratingEval = allAttrsEval.getVariableIteratingEvaluator();
                    if (iteratingEval instanceof MultiNamedAttributeEvaluator) {
                        variables.addAll(((MultiNamedAttributeEvaluator) iteratingEval).getAttributeNames());
                    } else if (iteratingEval instanceof MultiMatchAttributeEvaluator) {
                        return VariableImpact.ALWAYS_IMPACTED;
                    }
                } else if (evaluator instanceof AnyAttributeEvaluator) {
                    final AnyAttributeEvaluator allAttrsEval = (AnyAttributeEvaluator) evaluator;
                    final MultiAttributeEvaluator iteratingEval = allAttrsEval.getVariableIteratingEvaluator();
                    if (iteratingEval instanceof MultiNamedAttributeEvaluator) {
                        variables.addAll(((MultiNamedAttributeEvaluator) iteratingEval).getAttributeNames());
                    } else if (iteratingEval instanceof MultiMatchAttributeEvaluator) {
                        return VariableImpact.ALWAYS_IMPACTED;
                    }
                } else if (evaluator instanceof MappingEvaluator) {
                    final MappingEvaluator<?> allAttrsEval = (MappingEvaluator<?>) evaluator;
                    final MultiAttributeEvaluator iteratingEval = allAttrsEval.getVariableIteratingEvaluator();
                    if (iteratingEval instanceof MultiNamedAttributeEvaluator) {
                        variables.addAll(((MultiNamedAttributeEvaluator) iteratingEval).getAttributeNames());
                    }
                }
            }
        }

        final VariableImpact impact = new NamedVariableImpact(variables);
        this.variableImpact = impact;
        return impact;
    }
}
