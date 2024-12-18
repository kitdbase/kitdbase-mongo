import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';
dotenv.config();

const MONGODB_DATABASE = process.env.MONGODB_DATABASE;
const MONGODB_HOST = process.env.MONGODB_HOST;
const MONGODB_PORT = process.env.MONGODB_PORT;
const MONGODB_USER = process.env.MONGODB_USER;
const MONGODB_PASSWORD = process.env.MONGODB_PASSWORD;

class MongoDB {
    #client;
    bName;
    db = null;
    static instance;

    constructor() {
        if (!MongoDB.instance) {
            try {
                const url = `mongodb://${MONGODB_USER}:${MONGODB_PASSWORD}@${MONGODB_HOST}:${MONGODB_PORT}?authSource=admin`;
                this.#client = new MongoClient(url);
                this.dbName = `${MONGODB_DATABASE}`;
                MongoDB.instance = this;
            } catch (error) {
                console.error('Error en la inicialización de MongoDB:', error);
            }
        }

        return MongoDB.instance;
    }

    collection(name, excludeFields = []) {
      return new CollectionQuery(name, this.dbName, this.#client, this.db, excludeFields);
    }
    
}// MongoDB


class CollectionQuery {
  collectionName;
  dbName;
  db = null;
  filter = {}; // Almacena las condiciones de MongoDB
  projection = {}; // Proyección de campos
  sort = {}; // Ordenamiento
  limitValue = null;
  skipValue = null;
  excludeFields;
  #client;

  constructor(name, dbName, client, db = null, excludeFields = []) {
      this.collectionName = name;
      this.dbName = dbName;
      this.db = db; 
      this.#client = client; 
      this.excludeFields = excludeFields;
  }

  async connect() {
      if (!this.db) {
          await this.#client.connect();
          this.db = this.#client.db(this.dbName);
      }
      return this.db.collection(this.collectionName);
  }

  async drop() {
    try {
        const db = await this.connect(); // Aseguramos la conexión a la base de datos
        await db.drop(); // Eliminamos la colección
        return true; // Indicamos que la operación fue exitosa
    } catch (error) {
        throw new Error('Error al eliminar la colección: ' + error.message);
    }
  }

  select(fields = []) {
    this.projection = fields.reduce((proj, field) => {
        proj[field] = 1;
        return proj;
    }, {});
    return this;
  }

  where(column, operator, value) {
    switch (operator) {
        case '=':
            this.filter[column] = value;
            break;
        case '>':
            this.filter[column] = { $gt: value };
            break;
        case '<':
            this.filter[column] = { $lt: value };
            break;
        case '>=':
            this.filter[column] = { $gte: value };
            break;
        case '<=':
            this.filter[column] = { $lte: value };
            break;
        case '!=':
            this.filter[column] = { $ne: value };
            break;
        default:
            throw new Error(`Operador no soportado: ${operator}`);
    }
    return this;
  }

  // orWhere: condiciones con lógica OR
  orWhere(column, operator, value) {
      const orCondition = {};
      this.where(column, operator, value);
      this.filter = { $or: [this.filter, orCondition] };
      return this;
  }

  // whereIn: valores dentro de un array
  whereIn(column, values) {
      this.filter[column] = { $in: values };
      return this;
  }

  // whereNotIn: valores fuera de un array
  whereNotIn(column, values) {
      this.filter[column] = { $nin: values };
      return this;
  }

  // whereNull: verifica campos nulos
  whereNull(column) {
      this.filter[column] = null;
      return this;
  }

  // whereNotNull: verifica campos no nulos
  whereNotNull(column) {
      this.filter[column] = { $ne: null };
      return this;
  }

  // whereBetween: valores entre un rango
  whereBetween(column, [value1, value2]) {
      this.filter[column] = { $gte: value1, $lte: value2 };
      return this;
  }

  // orderBy: ordena resultados
  orderBy(column, direction) {
      this.sort[column] = direction === 'ASC' ? 1 : -1;
      return this;
  }

  // limit: limita la cantidad de resultados
  limit(number) {
      this.limitValue = number;
      return this;
  }

  // skip: salta resultados (para paginación)
  page(page, size) {
      this.skipValue = (page - 1) * size;
      this.limitValue = size;
      return this;
  }

  async count() {

    const collection = await this.connect();
    return await collection.countDocuments(this.filter);
  }

  async get() {
      const collection = await this.connect();
      let cursor = collection.find(this.filter);

      if (Object.keys(this.projection).length > 0) {
          cursor = cursor.project(this.projection);
      }

      if (Object.keys(this.sort).length > 0) {
          cursor = cursor.sort(this.sort);
      }

      if (this.limitValue !== null) {
          cursor = cursor.limit(this.limitValue);
      }

      if (this.skipValue !== null) {
          cursor = cursor.skip(this.skipValue);
      }

      return await cursor.toArray();
  }

  async insert(newData){
    try {
        const collection = await this.connect(); // Obtenemos la colección

        if (Array.isArray(newData)) {
            await collection.insertMany(newData); // Insertamos múltiples documentos
        } else {
            await collection.insertOne(newData); // Insertamos un único documento
        }

        return newData; // Devolvemos los datos insertados
    } catch (error) {
        console.error('Error al guardar los datos:', error);
        return null; // En caso de error, retornamos null
    }
  }

  async first() {
    try {
        const collection = await this.connect();
        const result = await collection.findOne(this.filter); // Usa el filtro directamente
        return result || null; // Devuelve el primer documento encontrado o null
    } catch (error) {
        throw new Error('Error al obtener el primer resultado: ' + error.message);
    }
  }

  async find(value, column = 'id') {
    try {
        const query = { [column]: value }; // Construimos el filtro dinámicamente
        const result = await this.collection.findOne(query); // Obtenemos el documento
        return result || null;
    } catch (error) {
        throw new Error('Error al encontrar el registro: ' + error.message);
    }
  }

  async update(data) {
    if (typeof data !== 'object' || Array.isArray(data)) {
        throw new Error('El método update requiere un objeto con pares clave-valor.');
    }

    if (Object.keys(this.filter).length === 0) {
        throw new Error('Debe especificar al menos una condición WHERE para realizar un update.');
    }

    try {
        const collection = await this.connect();
        const result = await collection.updateMany(this.filter, { $set: data });
        return result.modifiedCount; // Número de documentos modificados
    } catch (error) {
        throw new Error('Error al actualizar los datos: ' + error.message);
    }
  }

  async delete() {
    if (Object.keys(this.filter).length === 0) {
        throw new Error('Debe especificar al menos una condición WHERE para realizar un delete.');
    }

    try {
        const collection = await this.connect();
        const result = await collection.deleteMany(this.filter);
        return result.deletedCount; // Número de documentos eliminados
    } catch (error) {
        throw new Error('Error al eliminar los datos: ' + error.message);
    }
  }
}

const db = new MongoDB();
export default db;