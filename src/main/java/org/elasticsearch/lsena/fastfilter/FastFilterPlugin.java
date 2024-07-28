/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.elasticsearch.lsena.fastfilter;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.*;
import org.elasticsearch.script.FilterScript.LeafFactory;
import org.elasticsearch.search.lookup.SearchLookup;
import org.roaringbitmap.RoaringBitmap;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * RoaringBitmap plugin that allows filtering documents
 * using a base64 encoded roaringbitmap list of integers.
 */
public class FastFilterPlugin extends Plugin implements ScriptPlugin {

    private static final Logger logger = LogManager.getLogger(FastFilterPlugin.class);

    @Override
	public ScriptEngine getScriptEngine(
			Settings settings,
			Collection<ScriptContext<?>> contexts
			) {
		return new MyFastFilterEngine();
	}

	// tag::fast_filter
	private static class MyFastFilterEngine implements ScriptEngine {
		@Override
		public String getType() {
			return "fast_filter";
		}

		@Override
		public <T> T compile(
				String scriptName,
				String scriptSource,
				ScriptContext<T> context,
				Map<String, String> params
				) {
			if (!context.equals(FilterScript.CONTEXT)) {
				throw new IllegalArgumentException(getType()
						+ " scripts cannot be used for context ["
						+ context.name + "]");
			}
			// we use the script "source" as the script identifier
			// in this case, we use the name fast_filter
			if ("fast_filter".equals(scriptSource)) {
				FilterScript.Factory factory = new FastFilterFactory();
				return context.factoryClazz.cast(factory);
			}
			throw new IllegalArgumentException("Unknown script name "
					+ scriptSource);
		}

		@Override
		public void close() {
			// optionally close resources
		}

		@Override
		public Set<ScriptContext<?>> getSupportedContexts() {
			return Set.of(ScoreScript.CONTEXT);
		}

		private static class FastFilterFactory implements FilterScript.Factory,
		ScriptFactory {
			@Override
			public boolean isResultDeterministic() {
				// FastFilterLeafFactory only uses deterministic APIs, this
				// implies the results are cacheable.
				return true;
			}

			@Override
			public LeafFactory newFactory(
					Map<String, Object> params,
					SearchLookup lookup
					) {
				final byte[] decodedTerms = Base64.getDecoder().decode(params.get("terms").toString());
				final ByteBuffer buffer = ByteBuffer.wrap(decodedTerms);
				final String type = params.get("type").toString();
				RoaringBitmap rBitmap = new RoaringBitmap();
				try {
					rBitmap.deserialize(buffer);
				}
				catch (IOException e) {
					// Do something here
					throw ExceptionsHelper.convertToElastic(e);
				}
				if (type.equalsIgnoreCase("string")) {
					logger.debug("init string filter");
					return new FastFilterStringLeafFactory(params, lookup, rBitmap);
				}
				else {
					logger.debug("init int filter");
					return new FastFilterIntLeafFactory(params, lookup, rBitmap);
				}
			}
		}


		/**
		 * filter field is string type
		 */
		private static class FastFilterStringLeafFactory implements LeafFactory {
			private final Map<String, Object> params;
			private final SearchLookup lookup;
			private final String fieldName;
			private final String opType;
			private final RoaringBitmap rBitmap;
			private final boolean include;
			private final boolean exclude;

            private static final Logger logger = LogManager.getLogger(FastFilterStringLeafFactory.class);

            private FastFilterStringLeafFactory(Map<String, Object> params, SearchLookup lookup, RoaringBitmap rBitmap) {
				if (!params.containsKey("field")) {
					throw new IllegalArgumentException(
							"Missing parameter [field]");
				}
				if (!params.containsKey("terms")) {
					throw new IllegalArgumentException(
							"Missing parameter [terms]");
				}
				this.params = params;
				this.lookup = lookup;
				this.rBitmap = rBitmap;
				opType = params.get("operation").toString();
				fieldName = params.get("field").toString();
				include = opType.equals("include");
				exclude = !include;
			}


			@Override
			public FilterScript newInstance(DocReader docReader)
					throws IOException {

				return new FilterScript(params, lookup, docReader) {

					@Override
					public boolean execute() {
						try {
							logger.debug("retrieving doc values");

							final ScriptDocValues.Strings docValues = 
								(ScriptDocValues.Strings)getDoc().get(fieldName);
                            final int docValCnt = docValues.size();
							logger.debug("string docValCnt: " + docValCnt);

                            for (int i = 0; i < docValCnt; i++) {
                                logger.debug("orig string: " + docValues.get(i));
                                final int docVal = Math.toIntExact(Long.parseLong(docValues.get(i)));
								logger.debug("checking string docval: " + docVal);

                                if (exclude && rBitmap.contains(docVal)) {
									logger.debug("exclude string match: " + docVal);
                                    return false;
                                }
                                if (include && rBitmap.contains(docVal)) {
									logger.debug("include string match: " + docVal);
                                    return true;
                                }
                            }
                            return !include;
						}
						catch (NumberFormatException e) {
							throw ExceptionsHelper.convertToElastic(e);
						}
					}
				};
			}
		}


		/**
		 * filter field is numeric type
		 */
		private static class FastFilterIntLeafFactory implements LeafFactory {
			private final Map<String, Object> params;
			private final SearchLookup lookup;
			private final String fieldName;
			private final String opType;
			private final RoaringBitmap rBitmap;
			private final boolean include;
			private final boolean exclude;

            private static final Logger logger = LogManager.getLogger(FastFilterIntLeafFactory.class);

            private FastFilterIntLeafFactory(Map<String, Object> params, SearchLookup lookup, RoaringBitmap rBitmap) {
				if (!params.containsKey("field")) {
					throw new IllegalArgumentException(
							"Missing parameter [field]");
				}
				if (!params.containsKey("terms")) {
					throw new IllegalArgumentException(
							"Missing parameter [terms]");
				}
				this.params = params;
				this.lookup = lookup;
				this.rBitmap = rBitmap;
				opType = params.get("operation").toString();
				fieldName = params.get("field").toString();
				include = opType.equals("include");
				exclude = !include;
			}


			@Override
			public FilterScript newInstance(DocReader docReader)
					throws IOException {
				DocValuesDocReader dvReader = ((DocValuesDocReader) docReader);
				SortedNumericDocValues docValues = dvReader.getLeafReaderContext()
						.reader().getSortedNumericDocValues(fieldName);
				
				if (docValues == null) {
					logger.warn("null doc values for fastfilter");
					/*
					 * the field and/or docValues doesn't exist in this segment
					 */
					return new FilterScript(params, lookup, docReader) {
						@Override
						public boolean execute() {
							// return true when used as exclude filter
							return exclude;
						}
					};
				}

				return new FilterScript(params, lookup, docReader) {
					int currentDocid = -1;
					@Override
					public void setDocument(int docid) {
						/*
						 * advance has undefined behavior calling with
						 * a docid <= its current docid
						 */
						try {
							docValues.advance(docid);
						} catch (IOException e) {
							throw ExceptionsHelper.convertToElastic(e);
						}
						currentDocid = docid;
					}

					@Override
					public boolean execute() {
						try {
                            final int docValCnt = docValues.docValueCount();
							logger.debug("docValCnt: " + docValCnt);
                            for (int i = 0; i < docValCnt; i++) {
                                final int docVal = Math.toIntExact(docValues.nextValue());
								logger.debug("checking docval: " + docVal);

                                if (exclude && rBitmap.contains(docVal)) {
									logger.debug("exclude match: " + docVal + "(docid: " + currentDocid + ")");
                                    return false;
                                }
                                if (include && rBitmap.contains(docVal)) {
									logger.debug("include match: " + docVal + "(docid: " + currentDocid + ")");
                                    return true;
                                }
                            }
                            return !include;
						} catch (IOException e) {
							throw ExceptionsHelper.convertToElastic(e);
						}

					}
				};
			}
		}
	}
	// end::fast_filter
}
