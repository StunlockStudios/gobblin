package gobblin.stunlock.schemaflattening;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class SchemaFlattener
{
	public static FlattenedSchema getFlattenedSchema(Schema sourceSchema, String targetArrayName, String outputSchemaName) throws Exception
	{
		JSONObject root = new JSONObject(sourceSchema.toString());
		JSONArray sourceFieldsArray = root.getJSONArray("fields");
		List<JSONObject> baseFields = new ArrayList<JSONObject>();
		
		JSONObject targetArrayRecord = null;

		// Parse schema. Identify base fields and arrays to fork.
		for (int i = 0; i < sourceFieldsArray.length(); i++)
		{
			JSONObject field = sourceFieldsArray.getJSONObject(i);
			Object fieldType = field.get("type");

			if (fieldType instanceof JSONObject && AvroTypeEquals("array", fieldType))
			{
				JSONObject subField = (JSONObject) fieldType;
				Object arraySchema = subField.get("items");

				if (arraySchema instanceof JSONObject && AvroTypeEquals("record", arraySchema))
				{
					String arrayName = field.getString("name");
					
					if(arrayName.equalsIgnoreCase(targetArrayName)) // Found target array to flatten.
						targetArrayRecord = (JSONObject)arraySchema;
				}
				else
				{
					throw new JSONException("Flatten AVRO Error. I have no idea how to handle this array.");
					// Just add a single field to the definition?
				}
			}
			else
			{
				baseFields.add(field);
			}
		}
		
		if(targetArrayName == null || targetArrayRecord == null)
			throw new Exception("Failed to parse Schema. No array with name " + targetArrayName + " found. Schema:\n" + sourceSchema.toString(true));

		return makeFlattenedSchema(sourceSchema, baseFields, targetArrayName, outputSchemaName, targetArrayRecord);
	}
	
	private static FlattenedSchema makeFlattenedSchema(Schema sourceSchema, List<JSONObject> baseFields, String arrayName, String outputSchemaName, JSONObject arrayAvroRecord) throws JSONException
	{
		// Create Fork Schema
		JSONObject forkSchemaObject = new JSONObject(sourceSchema.toString());
		JSONArray fields = new JSONArray();
		for (JSONObject baseField : baseFields)
		{
			fields.put(baseField);
		}
		
		//String forkObjectName = arrayAvroRecord.getString("name");
		JSONArray fieldsArray = arrayAvroRecord.getJSONArray("fields");

		for (int o = 0; o < fieldsArray.length(); o++)
		{
			JSONObject field = fieldsArray.getJSONObject(o);
			field.put("name", arrayName + "_" + field.get("name"));
			fields.put(field);
		}

		forkSchemaObject.put("name", outputSchemaName);
		forkSchemaObject.put("fields", fields);

		FlattenedSchema flatSchema = new FlattenedSchema();
		flatSchema.SchemaArrayName = arrayName;
		flatSchema.Schema = new Schema.Parser().parse(forkSchemaObject.toString());

		return flatSchema;
	}
	
	public static int getSchemaArrayCount(Schema sourceSchema) throws JSONException
	{
		int arrayCount=0;
		JSONObject root = new JSONObject(sourceSchema.toString());
		JSONArray sourceFieldsArray = root.getJSONArray("fields");
		
		// Parse schema. Identify array count to fork.
		for (int i = 0; i < sourceFieldsArray.length(); i++)
		{
			JSONObject field = sourceFieldsArray.getJSONObject(i);
			Object fieldType = field.get("type");

			if (fieldType instanceof JSONObject && AvroTypeEquals("array", fieldType))
			{
				JSONObject subField = (JSONObject) fieldType;
				Object arraySchema = subField.get("items");

				if (arraySchema instanceof JSONObject && AvroTypeEquals("record", arraySchema))
				{
					arrayCount++;
				}
			}
		}
		return arrayCount;
	}

/*
	public static List<FlattenedSchema> getFlattenedSchemas(Schema sourceSchema) throws JSONException
	{
		JSONObject root = new JSONObject(sourceSchema.toString());
		JSONArray sourceFieldsArray = root.getJSONArray("fields");
		List<JSONObject> baseFields = new ArrayList<JSONObject>();
		Map<String, JSONObject> arrayFields = new HashMap<String, JSONObject>();

		// Parse schema. Identify base fields and arrays to fork.
		for (int i = 0; i < sourceFieldsArray.length(); i++)
		{
			JSONObject field = sourceFieldsArray.getJSONObject(i);
			Object fieldType = field.get("type");

			if (fieldType instanceof JSONObject && AvroTypeEquals("array", fieldType))
			{
				JSONObject subField = (JSONObject) fieldType;
				Object arraySchema = subField.get("items");

				if (arraySchema instanceof JSONObject && AvroTypeEquals("record", arraySchema))
				{
					String arrayFieldName = field.getString("name");
					arrayFields.put(arrayFieldName, (JSONObject)arraySchema);
				}
				else
				{
					throw new JSONException("Flatten AVRO Error. I have no idea how to handle this array.");
					// Just add a single field to the definition?
				}
			}
			else
			{
				baseFields.add(field);
			}
		}

		// Create Fork Schemas
		List<FlattenedSchema> forks = new ArrayList<FlattenedSchema>();
		for (Map.Entry<String, JSONObject> forkArray : arrayFields.entrySet())
		{
			FlattenedSchema newSchema = makeFlattenedSchema(sourceSchema, baseFields, forkArray.getKey(), forkArray.getValue());
			forks.add(newSchema);
		}
		return forks;
	}*/

	private static Boolean AvroTypeEquals(String str, Object avroObject) throws JSONException
	{
		if(avroObject instanceof JSONObject == false)
			return false;
		
		Object typeObj = ((JSONObject)avroObject).get("type");			
		return typeObj instanceof String && ((String)typeObj).equalsIgnoreCase(str);
	}
}