// queries.js
const { MongoClient } = require("mongodb");

const uri = "mongodb://127.0.0.1:27017";
const client = new MongoClient(uri);

async function runQueries() {
  try {
    await client.connect();
    console.log("✅ Connected to MongoDB");

    const db = client.db("plp_bookstore");
    const books = db.collection("books");

    console.log("\n📌 CRUD");

    // Find all books in a specific genre
    const programmingBooks = await books.find({ genre: "Programming" }).toArray();
    console.log("\n📚 Programming Books:");
    console.log(programmingBooks);

    // Find books published after 1900
    const booksAfter2010 = await books.find({ published_year: { $gt: 2010 } }).toArray();
    console.log("\n📅 Books published after 1900:");
    console.log(booksAfter2010);

    // Find books by author
    const booksByAuthor = await books.find({ author: "Robert C. Martin" }).toArray();
    console.log("\n👨‍💻 Books by Robert C. Martin:");
    console.log(booksByAuthor);

    // Update a book’s price
    const updatePrice = await books.updateOne(
      { title: "Clean Code" },
      { $set: { price: 29.99 } }
    );
    console.log(`\n💰 Updated Clean Code price:`, updatePrice.modifiedCount > 0 ? "Success" : "No changes");

    // Delete a book
    const deleteBook = await books.deleteOne({ title: "Clean Code" });
    console.log(`🗑️ Deleted 'Clean Code':`, deleteBook.deletedCount > 0 ? "Success" : "Not found");

    console.log("\n📌 Advanced Queries");

    // In-stock and published after 2010
    const inStockRecent = await books.find({
      in_stock: true,
      published_year: { $gt: 2010 },
    }).toArray();
    console.log("\n📦 In-stock books published after 2010:");
    console.log(inStockRecent);

    // Projection
    const projection = await books.find({}, {
      projection: { title: 1, author: 1, price: 1, _id: 0 },
    }).toArray();
    console.log("\n📝 Projection (title, author, price):");
    console.log(projection);

    // Sort ascending by price
    const priceAsc = await books.find().sort({ price: 1 }).toArray();
    console.log("\n📈 Books sorted by price (asc):");
    console.log(priceAsc);

    // Sort descending by price
    const priceDesc = await books.find().sort({ price: -1 }).toArray();
    console.log("\n📉 Books sorted by price (desc):");
    console.log(priceDesc);

    // Pagination (page 1, 5 per page)
    const page1Books = await books.find().skip(0).limit(5).toArray();
    console.log("\n📄 Page 1 (5 books):");
    console.log(page1Books);

    console.log("\n📌 Aggregation");

    // Average price by genre
    const avgPriceByGenre = await books.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } },
    ]).toArray();
    console.log("\n📊 Average Price by Genre:");
    console.log(avgPriceByGenre);

    // Author with most books
    const topAuthor = await books.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 },
    ]).toArray();
    console.log("\n🏆 Author with Most Books:");
    console.log(topAuthor);

    // Group by decade
    const groupByDecade = await books.aggregate([
      {
        $group: {
          _id: { $floor: { $divide: ["$published_year", 10] } },
          count: { $sum: 1 },
        },
      },
      {
        $project: {
          decade: { $multiply: ["$_id", 10] },
          count: 1,
          _id: 0,
        },
      },
    ]).toArray();
    console.log("\n📚 Books grouped by decade:");
    console.log(groupByDecade);

    console.log("\n📌 Indexing");

    // Index on title
    const indexTitle = await books.createIndex({ title: 1 });
    console.log("\n🔍 Created index on 'title':", indexTitle);

    // Compound index on author + published_year
    const indexCompound = await books.createIndex({ author: 1, published_year: -1 });
    console.log("🔍 Created compound index on 'author' + 'published_year':", indexCompound);

    // Performance analysis
    const explain = await books.find({ title: "Clean Code" }).explain("executionStats");
    console.log("\n📈 Performance Analysis (executionStats):");
    console.log(JSON.stringify(explain.executionStats, null, 2));

  } catch (err) {
    console.error("❌ Error running queries:", err);
  } finally {
    await client.close();
    console.log("\n🔌 Connection closed");
  }
}

runQueries();
