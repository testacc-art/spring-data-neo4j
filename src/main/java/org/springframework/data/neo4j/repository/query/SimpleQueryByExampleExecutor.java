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
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.neo4j.core.FluentFindOperation;
import org.springframework.data.neo4j.core.Neo4jOperations;
import org.springframework.data.neo4j.core.mapping.CypherGenerator;
import org.springframework.data.neo4j.core.mapping.Neo4jMappingContext;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.repository.query.QueryByExampleExecutor;
import org.springframework.data.support.PageableExecutionUtils;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.neo4j.cypherdsl.core.Cypher.asterisk;

/**
 * A fragment for repositories providing "Query by example" functionality.
 *
 * @author Michael J. Simons
 * @author Ján Šúr
 * @param <T> type of the domain class
 * @since 6.0
 */
@API(status = API.Status.INTERNAL, since = "6.0")
public final class SimpleQueryByExampleExecutor<T> implements QueryByExampleExecutor<T> {

	private final Neo4jOperations neo4jOperations;

	private final Neo4jMappingContext mappingContext;

	private final CypherGenerator cypherGenerator;

	public SimpleQueryByExampleExecutor(Neo4jOperations neo4jOperations, Neo4jMappingContext mappingContext) {

		this.neo4jOperations = neo4jOperations;
		this.mappingContext = mappingContext;
		this.cypherGenerator = CypherGenerator.INSTANCE;
	}

	@Override
	public <S extends T> Optional<S> findOne(Example<S> example) {
		return this.neo4jOperations.toExecutableQuery(example.getProbeType(),
				QueryFragmentsAndParameters.forExample(mappingContext, example)).getSingleResult();
	}

	@Override
	public <S extends T> List<S> findAll(Example<S> example) {
		return this.neo4jOperations.toExecutableQuery(example.getProbeType(),
				QueryFragmentsAndParameters.forExample(mappingContext, example)).getResults();
	}

	@Override
	public <S extends T> List<S> findAll(Example<S> example, Sort sort) {
		return this.neo4jOperations.toExecutableQuery(example.getProbeType(),
				QueryFragmentsAndParameters.forExample(mappingContext, example, sort)).getResults();
	}

	@Override
	public <S extends T> Page<S> findAll(Example<S> example, Pageable pageable) {

		List<S> page = this.neo4jOperations.toExecutableQuery(example.getProbeType(),
				QueryFragmentsAndParameters.forExample(mappingContext, example, pageable)).getResults();

		LongSupplier totalCountSupplier = () -> this.count(example);
		return PageableExecutionUtils.getPage(page, pageable, totalCountSupplier);
	}

	@Override
	public <S extends T> long count(Example<S> example) {

		Predicate predicate = Predicate.create(mappingContext, example);
		Statement statement = predicate.useWithReadingFragment(cypherGenerator::prepareMatchOf)
				.returning(Functions.count(asterisk())).build();

		return this.neo4jOperations.count(statement, predicate.getParameters());
	}

	@Override
	public <S extends T> boolean exists(Example<S> example) {
		return findAll(example).iterator().hasNext();
	}

	@Override
	public <S extends T, R> R findBy(Example<S> example, Function<FluentQuery.FetchableFluentQuery<S>, R> function) {

		if (this.neo4jOperations instanceof FluentFindOperation) {
			FetchableFluentQuerySupport<S, S> t = new FetchableFluentQuerySupport<>(example, example.getProbeType(),
					mappingContext, (FluentFindOperation) this.neo4jOperations, this::count, this::exists);
			return function.apply(t);
		}
		throw new UnsupportedOperationException(
				"Fluent find by example not supported with standard Neo4jOperations. Must support fluent queries too.");
	}
}
