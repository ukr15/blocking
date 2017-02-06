
package de.test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import rx.Observable;



public class TestVerticle extends AbstractVerticle {
	


	class Tuple{
		private int first;
		private String second;
		
		public Tuple(int first, String second){
			this.first = first;
			this.second = second;
		}

		public int getFirst() {
			return first;
		}
		

		public Tuple setFirst(int first) {
			this.first = first;
			return this;
		}

		public String getSecond() {
			return second;
		}

	}

	private static Logger logger;

	

	
	






	public void start() {	
		String propName = "vertx.options.warningExceptionTime";
		System.out.println(propName+": "+System.getProperty("vertx.options.warningExceptionTime"));
		logger = LoggerFactory.getLogger(TestVerticle.class);
		vertx.executeBlocking(future -> {
			future.complete();

		}, res -> {
			if(res.failed()){
				return;
			}
			test0();
		});

	}
	

	public void stop() {
	}


	private void test0() {
		Observable.range(0, 20000)
		.flatMap(person -> vertx.executeBlockingObservable(_future ->{
			_future.complete(test1(person));
		}))
		.buffer(1000)
		.flatMap(params -> {
			Observable<Integer> resultObservable = Observable.from(test2());
			 return Observable.zip(resultObservable, Observable.from(params), (first, second) -> Observable.just(new Tuple(first, ((JsonArray) second).getString(0)))).flatMap(x->x).collect(()->new ArrayList<Tuple>(), (data, item)-> data.add(item));

			})
		.count()		
		.collect(()-> { 
			long [] counter = new long [1];
			counter[0] = 0;
			return counter;
		}, (counter, item)->{
			counter[0] += item; 
			})
		.subscribe(insertedRecords->{
				logger.info(String.format("records added: %s", insertedRecords[0]));
				
		}, t->{
			logger.error(t.toString());
			t.printStackTrace();
		});

		

}

	private JsonArray test1(long id){
		final JsonArray params = new JsonArray();
		long count= 0;
		for(int i =0; i < 10000; ++i){
			++count;
		}
		params.add(UUID.randomUUID().toString())
		.add("{AICEAACAAACRAAAAEABBABAAAAARAQAAAAIBBgAAAAIAEAACBAIAQgBABAAAQAEABEAAAARIBAAIABAICAAA}")
		.add("{gQAGQgCABQYAAEAJAAxEAggQQCABhBAQDIAACAAAIASkKACAAYAASAABgGAAAA5ggEABYIIQASAEBAEgJQxUYAgBGCJBACARQAIAAQYAAMEAIAAAIAwgEhADgAAgiBAAIAAAAAMFEGICAYQCAkAAUFooABACAVAgCqAQAAI=}")
		.add("{AAAUAAgACAASAAAAAARgAAAAGAAAAQgQAAAEBCAAAgAIEAEAAJAAQABAIAAAIAlAABAAEAQIAAAgBBAAAEIA}")
		.add("{AAACAACiAAIQIAIAAgIAAgAAICAAAqAAKAgCQAACAAAAIAAgAgIAAAAAikAACAAAICgIACIAIAAAAAgAAiggASAgAAIAACAgAggggAAAIKAgAAQAIAAgIAACACAAgAAgAAoEgIAAAgIgAAAAgAAgAAgCIICAMAAgACAAgIA}")
		.add(15)
		.add(6)
		.add(count)
	    .add("IAAIQAAAJAAIAAQAYAAAAEAAIAQAAAAEIEAAAIBAIAAAAIAAJCAAAqAAQiAAAqAAAiQAAqAAAmAAAqBAAAAE")
		.add("CAiEAACIAAggAgAAEBAJgAAgIBAAJAAAAAAAAFIBACSBQCEUiAQTgUAgEggFQQAQgAEAgAIBgAAAigABhAAAgQEAgBAIhJAAAABBAEkAAAEACiCgAIAEAAABQkgAAEFAECgAgAAgAAAhBMABAoEAoIAAQIIAAZADBIACBKE=")
		.add("UNKNOWN")
	   .add("{foo}").add("{baz}").add("foobaz");
		return params;
	}
	private List<Integer> test2(){
		List<Integer> iList = new ArrayList<Integer>();
		for(int i = 0; i< 10000; ++i){
			iList.add(i);
		}
		return iList;
	}

}
