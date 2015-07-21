using BitSharp.Core.Domain;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Builders
{
    public class BlockSpentTxesBuilder
    {
        private readonly SortedDictionary<int, List<SpentTx>> spentTxesByBlock = new SortedDictionary<int,List<SpentTx>>();

        public void AddSpentTx(SpentTx spentTx)
        {
            var blockIndex = spentTx.ConfirmedBlockIndex;

            List<SpentTx> spentTxes;
            if (!spentTxesByBlock.TryGetValue(blockIndex, out spentTxes))
            {
                spentTxes = new List<SpentTx>();
                spentTxesByBlock.Add(blockIndex, spentTxes);
            }

            spentTxes.Add(spentTx);
        }

        public BlockSpentTxes ToImmutable()
        {
            return new BlockSpentTxes(ImmutableList.CreateRange(spentTxesByBlock.SelectMany(x => x.Value)));
        }
    }
}
