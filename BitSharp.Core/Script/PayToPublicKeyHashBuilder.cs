using BitSharp.Common;

namespace BitSharp.Core.Script
{
    public class PayToPublicKeyHashBuilder
    {
        public byte[] CreateOutputFromPublicKey(byte[] publicKey)
        {
            var publicKeyHash = RIPEMD160Static.ComputeHash(SHA256Static.ComputeHash(publicKey));
            return CreateOutputFromPublicKeyHash(publicKeyHash);
        }

        public byte[] CreateOutputFromPublicKeyHash(byte[] publicKeyHash)
        {
            using (var outputScript = new ScriptBuilder())
            {
                outputScript.WriteOp(ScriptOp.OP_DUP);
                outputScript.WriteOp(ScriptOp.OP_HASH160);
                outputScript.WritePushData(publicKeyHash);
                outputScript.WriteOp(ScriptOp.OP_EQUALVERIFY);
                outputScript.WriteOp(ScriptOp.OP_CHECKSIG);

                return outputScript.GetScript();
            }
        }
    }
}
