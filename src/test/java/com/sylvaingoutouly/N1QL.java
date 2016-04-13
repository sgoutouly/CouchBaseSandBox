package com.sylvaingoutouly;

import static com.couchbase.client.java.query.N1qlQuery.simple;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static java.lang.System.err;
import static java.lang.System.out;
import static org.junit.Assert.assertEquals;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Required;

import rx.Observable;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;

import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.Index;
import com.couchbase.client.java.query.N1qlQueryResult;


public class N1QL {

	private Bucket beerSample;
	private CouchbaseCluster cluster;
	
	
	@Test public void selectOnBeerSync() {		
		// Simple select on sample beer bucket
		N1qlQueryResult beers = beerSample.query(simple(select("*").from(i("beer-sample")).limit(10)));
		
		assertEquals(10, beers.info().resultCount());
		
		out.println(beers.errors().toString());
		out.println(beers.info().elapsedTime());
		out.println(beers.info().resultCount());
	}
	
	@Test public void selectOnBeerAsync() {		
		// Simple select on sample beer async bucket
		Observable<AsyncN1qlQueryResult> beers = beerSample.async().query(simple(select("*").from(i("beer-sample")).limit(10)));
		// Handling async result
		beers.subscribe(result -> {
			result.errors().subscribe(errors -> out.println(errors.toString()));
			result.info().subscribe(info -> {
				assertEquals(10, info.resultCount());	
				out.println(info.elapsedTime());
				out.println(info.resultCount());
			});
		});	
		beers.toBlocking().singleOrDefault(null); // We block to force junit into waiting for the result
	}
	
    @RequiredArgsConstructor(staticName = "of") @Getter
	private static class ResultHolder {
		private final List<JsonObject> results;
		private final List<JsonObject> errors;
		private final boolean parseStatus;

	}
	
	@Test public void selectOnBeerAsyncAdvanced() {		
		// Simple select on sample beer async bucket
		Observable<AsyncN1qlQueryResult> beers = beerSample.async().query(simple(select("*").from(i("beer-sample")).limit(10)));
		
		Observable<ResultHolder> holder = Observable.zip(
				beers.map(AsyncN1qlQueryResult::parseSuccess),
				beers.flatMap(AsyncN1qlQueryResult::rows).map(AsyncN1qlQueryRow::value).toList()				,
				beers.flatMap(results -> results.errors()).toList(),
				(s, jr, je) -> ResultHolder.of(jr, je, s));

		holder.subscribe(queryResult -> {
			System.err.println("RESULTS : " + queryResult.getResults());
			System.err.println("ERRORS : " + queryResult.getErrors());
			System.err.println("STATUS : " + queryResult.isParseStatus());
		});


		holder.toBlocking().singleOrDefault(null); // We block to force junit into waiting for the result
	}
	
	
	@Before public void before() {
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.create();
		cluster = CouchbaseCluster.create(env);
		beerSample = cluster.openBucket("beer-sample");
		// Needed to use N1QL
		beerSample.query(simple(Index.createPrimaryIndex().on("beer-sample")));
	}
	
	@After public void after() {
		cluster.disconnect();
	}

}
