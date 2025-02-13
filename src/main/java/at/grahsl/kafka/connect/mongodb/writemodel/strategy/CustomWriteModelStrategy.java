package at.grahsl.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.ReplaceOneModel;
import org.bson.*;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import org.apache.kafka.connect.errors.DataException;

public class CustomWriteModelStrategy implements WriteModelStrategy{
    private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);

    //incoming json should have one message key e.g. { "message": "Hello World"}
    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {

        // Retrieve the value part of the SinkDocument
//        BsonDocument vd = document.getValueDoc().orElseThrow(
//                () -> new DataException("Error: cannot build the WriteModel since the value document was missing unexpectedly"));
//
//        System.out.println("1111111111111111111111111111111111111111111111111111111111111111111111111111111111");
//        System.out.println("1111111111111111111111111111111111111111111111111111111111111111111111111111111111");
//        System.out.println("1111111111111111111111111111111111111111111111111111111111111111111111111111111111");
//        // extract message from incoming document
//        BsonString message = new BsonString("");
//        if (vd.containsKey("message")) {
//            message = vd.get("message").asString();
//        }
//
//        // Define the filter part of the update statement
//        BsonDocument filters = new BsonDocument("counter", new BsonDocument("$lt", new BsonInt32(10)));
//
//        // Define the update part of the update statement
//        BsonDocument updateStatement = new BsonDocument();
//        updateStatement.append("$inc", new BsonDocument("counter", new BsonInt32(1)));
//        updateStatement.append("$push", new BsonDocument("messages", new BsonDocument("message", message)));
//
//        // Return the full update å
//        return new UpdateOneModel<BsonDocument>(
//                filters,
//                updateStatement,
//                UPDATE_OPTIONS
//        );

        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the value document was missing unexpectedly")
        );

        BsonValue businessKey = vd.get(DBCollection.ID_FIELD_NAME);

        if(businessKey == null || !(businessKey instanceof BsonDocument)) {
//            throw new DataException("error: cannot build the WriteModel since"
//                    + " the value document does not contain an _id field of type BsonDocument"
//                    + " which holds the business key fields");
            businessKey = vd.get("id");
        }
        vd.remove(DBCollection.ID_FIELD_NAME);
        return new ReplaceOneModel<>(new BsonDocument(DBCollection.ID_FIELD_NAME,businessKey), vd, UPDATE_OPTIONS);

    }
}
