using System.Collections;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.QueryParsers;
using NUnit.Framework;

namespace NHibernate.Search.Tests.Analyzer
{
    [TestFixture]
    public class AnalyzerTest : SearchTestCase
    {
        protected override IList Mappings
        {
            get { return new string[] { "Analyzer.MyEntity.hbm.xml" }; }
        }

        [Test]
        public void TestScopedAnalyzers()
        {
            using (var session = OpenSession())
            using (var transaction = session.BeginTransaction())
            using (var fullTextSession = Search.CreateFullTextSession(session))
            {
                using (var ftsTransaction = fullTextSession.BeginTransaction())
                {
                    fullTextSession.Save(new MyEntity
                    {
                        Entity = "alarm",
                        Field = "energy",
                        Property = "cat",
                        Component = new MyComponent
                        {
                            ComponentProperty = "noise"
                        }
                    });
                    fullTextSession.Flush();
                    ftsTransaction.Commit();
                }

                var parser = new QueryParser(Environment.LuceneVersion, "id", new StandardAnalyzer(Environment.LuceneVersion));

                using (var ftsTransaction = fullTextSession.BeginTransaction())
                {
                    var query1 = fullTextSession.CreateFullTextQuery(parser.Parse("entity:alarm"), typeof(MyEntity));
                    Assert.AreEqual(1, query1.ResultSize, "Entity query");

                    var query2 = fullTextSession.CreateFullTextQuery(parser.Parse("property:cat"), typeof(MyEntity));
                    Assert.AreEqual(1, query2.ResultSize, "Property query");

                    var query3 = fullTextSession.CreateFullTextQuery(parser.Parse("field:energy"), typeof(MyEntity));
                    Assert.AreEqual(1, query3.ResultSize, "Field query");

                    var query4 = fullTextSession.CreateFullTextQuery(parser.Parse("component.componentProperty:noise"));
                    Assert.AreEqual(1, query4.ResultSize, "Component query");

                    fullTextSession.Delete(query4.UniqueResult());

                    ftsTransaction.Commit();
                }

                fullTextSession.Close();
                transaction.Commit();
                session.Close();
            }
        }
    }
}