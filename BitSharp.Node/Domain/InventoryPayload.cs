using System.Collections.Immutable;

namespace BitSharp.Node.Domain
{
    public class InventoryPayload
    {
        public readonly ImmutableArray<InventoryVector> InventoryVectors;

        public InventoryPayload(ImmutableArray<InventoryVector> InventoryVectors)
        {
            this.InventoryVectors = InventoryVectors;
        }

        public InventoryPayload With(ImmutableArray<InventoryVector>? InventoryVectors = null)
        {
            return new InventoryPayload
            (
                InventoryVectors ?? this.InventoryVectors
            );
        }
    }
}
