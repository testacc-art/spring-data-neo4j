/*
 * Copyright 2011-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.neo4j.repository.query;

import org.apiguardian.api.API;
import org.neo4j.cypherdsl.core.Functions;
import org.neo4j.cypherdsl.core.Statement;
import org.reactivestreams.Publisher;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.neo4j.core.ReactiveNeo4jOperations;
import org.springframework.data.neo4j.core.mapping.CypherGenerator;
import org.springframework.data.neo4j.core.mapping.Neo4jMappingContext;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.repository.query.ReactiveQueryByExampleExecutor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.neo4j.cypherdsl.core.Cypher.asterisk;

import java.util.function.Function;

/**
 * A fragment for repositories providing "Query by example" functionality in a reactive way.
 *
 * @author Gerrit Meier
 * @author Michael J. Simons
 * @param <T> type of the domain class
 * @since 6.0
 */
@API(status = API.Status.INTERNAL, since = "6.0")
public final class SimpleReactiveQueryByExampleExecutor<T> implements ReactiveQueryByExampleExecutor<T> {

	private final ReactiveNeo4jOperations neo4jOperations;

	private final Neo4jMappingContext mappingContext;

	private final CypherGenerator cypherGenerator;

	public SimpleReactiveQueryByExampleExecutor(ReactiveNeo4jOperations neo4jOperations, Neo4jMappingContext mappingContext) {

		this.neo4jOperations = neo4jOperations;
		this.mappingContext = mappingContext;
		this.cypherGenerator = CypherGenerator.INSTANCE;
	}

	@Override
	public <S extends T> Mono<S> findOne(Example<S> example) {
		return this.neo4jOperations
				.toExecutableQuery(example.getProbeType(), QueryFragmentsAndParameters.forExample(mappingContext, example))
				.flatMap(ReactiveNeo4jOperations.ExecutableQuery::getSingleResult);
	}

	@Override
	public <S extends T> Flux<S> findAll(Example<S> example) {
		return this.neo4jOperations
				.toExecutableQuery(example.getProbeType(), QueryFragmentsAndParameters.forExample(mappingContext, example))
				.flatMapMany(ReactiveNeo4jOperations.ExecutableQuery::getResults);
	}

	@Override
	public <S extends T> Flux<S> findAll(Example<S> example, Sort sort) {
		return this.neo4jOperations
				.toExecutableQuery(example.getProbeType(), QueryFragmentsAndParameters.forExample(mappingContext, example, sort))
				.flatMapMany(ReactiveNeo4jOperations.ExecutableQuery::getResults);
	}

	@Override
	public <S extends T> Mono<Long> count(Example<S> example) {

		Predicate predicate = Predicate.create(mappingContext, example);
		Statement statement = predicate.useWithReadingFragment(cypherGenerator::prepareMatchOf)
				.returning(Functions.count(asterisk())).build();

		return this.neo4jOperations.count(statement, predicate.getParameters());
	}

	@Override
	public <S extends T> Mono<Boolean> exists(Example<S> example) {
		return findAll(example).hasElements();
	}

	@Override public <S extends T, R, P extends Publisher<R>> P findBy(
			Example<S> example,
			Function<FluentQuery.ReactiveFluentQuery<S>, P> function) {
		throw new UnsupportedOperationException();
	}
}
