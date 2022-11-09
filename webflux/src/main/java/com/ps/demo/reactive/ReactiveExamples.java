package com.ps.demo.reactive;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.json.JSONObject;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Timed;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Slf4j
public class ReactiveExamples {
    public static void main(String[] args){
        Path path = Paths.get(args[0]);

        log.info("Started");
        AtomicInteger linunumber = new AtomicInteger(0);

        //declare flux to read file
        ConnectableFlux<Tuple2<String, Integer>> reader = Flux.using(() -> Files.lines(path), 
            Flux::fromStream , 
            Stream::close)
            .doOnEach( signal -> log.info("got signal {}", signal.getType()) )
            .doOnNext( i -> log.info("Got line: " + linunumber.incrementAndGet()))
            .doOnComplete( () -> log.info("Processing completed"))
            .parallel().runOn(Schedulers.boundedElastic())
            .map( line -> new JSONObject(line) )
            .map( jo -> Tuples.of(jo.get("business_id").toString(), 1))
            .sequential()
            .onErrorContinue( (t, i) -> log.info("Error occured: " + t + " for item " + i))
            .groupBy( item -> item.getT1())
            .flatMap( g -> g.reduce( Tuples.of("", 0), 
                ( current, next) -> Tuples.of(next.getT1(), current.getT2() + next.getT2())))
            .publish();
        log.info("Flux initialized");

        Flux<Timed<Tuple2<String, Integer>>> timedReader = reader.share().timed();

        timedReader.subscribe(
            item -> System.out.println("Key: " + item.get().getT1() + "; elapsed time: " + item.elapsed()),
            error -> System.out.println("Error occured: " + error),
            () -> log.info("Timed stream Finished")
        );  
        log.info("Timed Subscription finished");    
        
        reader.subscribe(
            item -> System.out.println("Key: " + item.getT1() + "; value: " + item.getT2()),
            error -> System.out.println("Error occured: " + error),
            () -> log.info("Standard stream Finished")
        );    
        log.info("Normal Subscription finished");    
        reader.connect();
        log.info("Connect finished");  

        log.info("Reiterating source");  
        linunumber.set(0);
        reader.toIterable().forEach( item -> System.out.println("Key: " + item.getT1() + "; value: " + item.getT2()));
        log.info("Reiterating source finished");  
    }

}
