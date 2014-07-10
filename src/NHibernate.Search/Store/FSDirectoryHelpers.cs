using System.IO;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Store;

namespace NHibernate.Search.Store
{
    public static class FSDirectoryHelpers
    {
        /// <summary>
        /// Initializes the index at the given index
        /// </summary>
        public static void InitializeIndex(DirectoryInfo path, string indexName = null)
        {
            try
            {
                var directory = FSDirectory.Open(path);

                // Exit if the index already exists
                if (IndexReader.IndexExists(directory))
                {
                    return;
                }

                // TODO:RB: re-add logging
                //log.DebugFormat("Initialize index: '{0}'", path);

                var maxFieldLength = IndexWriter.MaxFieldLength.UNLIMITED;
                var policy = new KeepOnlyLastCommitDeletionPolicy();

                using (var analyzer = new StandardAnalyzer(Environment.LuceneVersion))
                using (var indexWriter = new IndexWriter(directory, analyzer, true, policy, maxFieldLength))
                {
                    // Do nothing, index writer's constructor has initialized the index
                }
            }
            catch (IOException e)
            {
                throw new HibernateException("Unable to initialize index: " + indexName ?? path.FullName, e);
            }
        }
    }
}
