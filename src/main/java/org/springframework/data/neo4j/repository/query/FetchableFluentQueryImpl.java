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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apiguardian.api.API;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.neo4j.core.FluentFindOperation;
import org.springframework.data.neo4j.core.mapping.Neo4jMappingContext;
import org.springframework.data.neo4j.core.mapping.PropertyFilter;
import org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.lang.Nullable;

/**
 * Immutable implementation of a {@link FetchableFluentQuery}. All
 * methods that return a {@link FetchableFluentQuery} return a new instance, the original instance won't be
 * modified.
 *
 * @author Michael J. Simons
 * @param <S> Source type
 * @param <R> Result type
 * @since 6.2
 */
@API(status = API.Status.INTERNAL, since = "6.2")
final class FetchableFluentQueryImpl<S, R> implements FetchableFluentQuery<R> {

	private final Neo4jMappingContext mappingContext;

	private final Example<S> example;

	private final Class<R> resultType;

	private final FluentFindOperation findOperation;

	private final Function<Example<S>, Long> countOperation;

	private final Function<Example<S>, Boolean> existsOperation;

	private final Sort sort;

	@Nullable
	private final Set<String> properties;

	FetchableFluentQueryImpl(
			Example<S> example,
			Class<R> resultType,
			Neo4jMappingContext mappingContext,
			FluentFindOperation findOperation,
			Function<Example<S>, Long> countOperation,
			Function<Example<S>, Boolean> existsOperation
	) {
		this(example, resultType, mappingContext, findOperation, countOperation, existsOperation, Sort.unsorted(),
				null);
	}

	FetchableFluentQueryImpl(
			Example<S> example,
			Class<R> resultType,
			Neo4jMappingContext mappingContext,
			FluentFindOperation findOperation,
			Function<Example<S>, Long> countOperation,
			Function<Example<S>, Boolean> existsOperation,
			Sort sort,
			@Nullable Collection<String> properties
	) {
		this.mappingContext = mappingContext;
		this.example = example;
		this.resultType = resultType;
		this.findOperation = findOperation;
		this.countOperation = countOperation;
		this.existsOperation = existsOperation;
		this.sort = sort;
		if (properties != null) {
			this.properties = new HashSet<>(properties);
		} else {
			this.properties = null;
		}
	}

	Predicate<PropertyFilter.RelaxedPropertyPath> createIncludedFieldsPredicate() {

		if (this.properties == null) {
			return path -> true;
		}
		return path -> this.properties.contains(path.toDotPath());
	}

	@Override
	@SuppressWarnings("HiddenField")
	public FetchableFluentQuery<R> sortBy(Sort sort) {

		return new FetchableFluentQueryImpl<>(example, resultType, mappingContext, this.findOperation,
				this.countOperation, this.existsOperation, this.sort.and(sort), this.properties);
	}

	@Override
	@SuppressWarnings("HiddenField")
	public <NR> FetchableFluentQuery<NR> as(Class<NR> resultType) {

		return new FetchableFluentQueryImpl<>(example, resultType, mappingContext, this.findOperation,
				this.countOperation, this.existsOperation);
	}

	@Override
	@SuppressWarnings("HiddenField")
	public FetchableFluentQuery<R> project(Collection<String> properties) {

		Set<String> newProperties = new HashSet<>();
		if (this.properties != null) {
			newProperties.addAll(this.properties);
		}
		newProperties.addAll(properties);
		return new FetchableFluentQueryImpl<>(example, resultType, mappingContext, this.findOperation,
				this.countOperation, this.existsOperation, sort, newProperties);
	}

	@Override
	public R one() {

		return this.findOperation.find(example.getProbeType())
				.as(resultType)
				.matching(QueryFragmentsAndParameters.forExample(mappingContext, example, sort,
						createIncludedFieldsPredicate()))
				.oneValue();
	}

	@Override
	public R first() {

		List<R> all = all();
		return all.isEmpty() ? null : all.get(0);
	}

	@Override
	public List<R> all() {

		return this.findOperation.find(example.getProbeType())
				.as(resultType)
				.matching(QueryFragmentsAndParameters.forExample(mappingContext, example, sort,
						createIncludedFieldsPredicate()))
				.all();
	}

	@Override
	public Page<R> page(Pageable pageable) {

		List<R> page = this.findOperation.find(example.getProbeType())
				.as(resultType)
				.matching(QueryFragmentsAndParameters.forExample(mappingContext, example, pageable,
						createIncludedFieldsPredicate()))
				.all();

		LongSupplier totalCountSupplier = this::count;
		return PageableExecutionUtils.getPage(page, pageable, totalCountSupplier);
	}

	@Override
	public Stream<R> stream() {
		return all().stream();
	}

	@Override
	public long count() {
		return countOperation.apply(example);
	}

	@Override
	public boolean exists() {
		return existsOperation.apply(example);
	}
}
