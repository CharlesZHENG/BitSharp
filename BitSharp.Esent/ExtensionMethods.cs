using Microsoft.Isam.Esent.Interop;

namespace BitSharp.Esent
{
    internal static class ExtensionMethods
    {
        public static Transaction BeginTransaction(this Session session)
        {
            return new Transaction(session);
        }

        public static void CommitLazy(this Transaction transaction)
        {
            transaction.Commit(CommitTransactionGrbit.LazyFlush);
        }

        public static Update BeginUpdate(this Session session, JET_TABLEID tableid, JET_prep prep)
        {
            return new Update(session, tableid, prep);
        }
    }
}
