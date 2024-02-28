package at.grahsl.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.bson.*;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import org.apache.kafka.connect.errors.DataException;

/**
 * @author GME
 * @version V1.0
 * @Package at.grahsl.kafka.connect.mongodb.writemodel.strategy
 * @date 2024/2/28 17:29
 * @Copyright © 2014-2023 码克布克网络工作室
 */
public class CustomWriteModelStrategy implements WriteModelStrategy{
    private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);

    //incoming json should have one message key e.g. { "message": "Hello World"}
    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {

        // Retrieve the value part of the SinkDocument
        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("Error: cannot build the WriteModel since the value document was missing unexpectedly"));

        // extract message from incoming document
        BsonString message = new BsonString("");
        if (vd.containsKey("message")) {
            message = vd.get("message").asString();
        }

        // Define the filter part of the update statement
        BsonDocument filters = new BsonDocument("counter", new BsonDocument("$lt", new BsonInt32(10)));

        // Define the update part of the update statement
        BsonDocument updateStatement = new BsonDocument();
        updateStatement.append("$inc", new BsonDocument("counter", new BsonInt32(1)));
        updateStatement.append("$push", new BsonDocument("messages", new BsonDocument("message", message)));

        // Return the full update å
        return new UpdateOneModel<BsonDocument>(
                filters,
                updateStatement,
                UPDATE_OPTIONS
        );
    }
}
