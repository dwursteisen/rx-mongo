
import org.mongodb.AsyncBlock;
import org.mongodb.Document;
import org.mongodb.MongoClient;
import org.mongodb.MongoClients;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoView;
import org.mongodb.QueryOptions;
import org.mongodb.connection.ServerAddress;
import rx.Observable;
import rx.Subscriber;

import java.net.UnknownHostException;


public class Main {


    public static void main(String[] args) throws UnknownHostException {
        MongoClient client = MongoClients.create(new ServerAddress("localhost"));
        MongoDatabase database = client.getDatabase("rx");
        MongoCollection<Document> collection = database.getCollection("test");


        RxSupport.from(collection.find())
                .map((doc) -> doc.get("title"))
                .subscribe((title) -> System.out.println("Title => " + title),
                        Throwable::printStackTrace,
                        () -> System.out.println("Completed !"));

        RxSupport.from(collection.find().get())
                .map((doc) -> doc.get("title"))
                .subscribe((title) -> System.out.println("(cursor) Title => " + title),
                        Throwable::printStackTrace,
                        () -> System.out.println("(cursor) Completed !"));


        Observable<Document> obs = new RxMongo(database).find();
        Observable<Document> obs2 = new RxMongo(database).find(new Document("_id", "aserty"));
        Observable<Document> obs3 = new RxMongo(database).find().limit(3);

    }

    public static class RxSupport {
        public static Observable<Document> from(MongoView<Document> view) {
            return Observable.create(new ForEachViewOperation(view));
        }

        public static Observable<Document> from(MongoCursor<Document> cursor) {
            return Observable.create(new ForEachCursorOperation(cursor));
        }
    }

    public static class ForEachCursorOperation implements Observable.OnSubscribe<Document> {
        private final MongoCursor<Document> cursor;

        public ForEachCursorOperation(MongoCursor<Document> cursor) {
            this.cursor = cursor;
        }

        @Override
        public void call(Subscriber<? super Document> subscriber) {
            while (cursor.hasNext()) {
                if (!subscriber.isUnsubscribed()) {
                    subscriber.onNext(cursor.next());
                }
            }
            if (!subscriber.isUnsubscribed()) {
                subscriber.onCompleted();
            }
        }
    }

    public static class ForEachViewOperation implements Observable.OnSubscribe<Document> {

        private final MongoView<Document> view;

        public ForEachViewOperation(MongoView<Document> view) {
            this.view = view;
        }

        @Override
        public void call(Subscriber<? super Document> subscriber) {
            view.asyncForEach(new AsyncBlock<Document>() {
                @Override
                public void done() {
                    if (!subscriber.isUnsubscribed()) {
                        subscriber.onCompleted();
                    }
                }

                @Override
                public void apply(Document document) {
                    if (!subscriber.isUnsubscribed()) {
                        subscriber.onNext(document);
                    }
                }
            });
        }
    }
}