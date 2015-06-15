using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using NLog;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.Sec;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Crypto.Signers;
using Org.BouncyCastle.Security;
using System;
using System.Collections.Immutable;
using System.IO;

namespace BitSharp.Core.Script
{
    public class TransactionManager
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public Tuple<ECPrivateKeyParameters, ECPublicKeyParameters> CreateKeyPair()
        {
            var curve = SecNamedCurves.GetByName("secp256k1");
            var domainParameters = new ECDomainParameters(curve.Curve, curve.G, curve.N, curve.H, curve.GetSeed());

            var generator = new ECKeyPairGenerator();
            generator.Init(new ECKeyGenerationParameters(domainParameters, new SecureRandom()));
            var keyPair = new MethodTimer(false).Time("GenerateKeyPair", () => generator.GenerateKeyPair());

            var privateKey = (ECPrivateKeyParameters)keyPair.Private;
            var publicKey = (ECPublicKeyParameters)keyPair.Public;

            //Debug.WriteLine("Private:  {0}".Format2(privateKey.D.ToHexNumberStringUnsigned()));
            //Debug.WriteLine("Public X: {0}".Format2(publicKey.Q.X.ToBigInteger().ToHexNumberStringUnsigned()));
            //Debug.WriteLine("Public Y: {0}".Format2(publicKey.Q.Y.ToBigInteger().ToHexNumberStringUnsigned()));

            return Tuple.Create(privateKey, publicKey);
        }

        public byte[] CreatePublicAddress(ECPublicKeyParameters publicKey)
        {
            var publicAddress =
                new byte[] { 0x04 }
                .Concat(publicKey.Q.X.ToBigInteger().ToByteArrayUnsigned())
                .Concat(publicKey.Q.Y.ToBigInteger().ToByteArrayUnsigned());

            //Debug.WriteLine("Public Address: {0}".Format2(publicAddress.ToHexDataString()));

            return publicAddress;
        }

        public byte[] CreatePublicKeyScript(ECPublicKeyParameters publicKey)
        {
            return CreatePublicKeyScript(CreatePublicAddress(publicKey));
        }

        public byte[] CreatePublicKeyScript(byte[] publicKey)
        {
            var publicKeyHash = RIPEMD160Static.ComputeHash(SHA256Static.ComputeHash(publicKey));

            using (var publicKeyScript = new ScriptBuilder())
            {
                publicKeyScript.WriteOp(ScriptOp.OP_DUP);
                publicKeyScript.WriteOp(ScriptOp.OP_HASH160);
                publicKeyScript.WritePushData(publicKeyHash);
                publicKeyScript.WriteOp(ScriptOp.OP_EQUALVERIFY);
                publicKeyScript.WriteOp(ScriptOp.OP_CHECKSIG);

                //Debug.WriteLine("Public Script: {0}".Format2(publicKeyScript.GetScript().ToHexDataString()));

                return publicKeyScript.GetScript();
            }
        }

        public byte[] CreatePrivateKeyScript(Transaction tx, int inputIndex, byte hashType, ECPrivateKeyParameters privateKey, ECPublicKeyParameters publicKey)
        {
            //TODO
            var scriptEngine = new ScriptEngine();

            var publicAddress = CreatePublicAddress(publicKey);
            var publicKeyScript = CreatePublicKeyScript(publicAddress);
            var txSignature = scriptEngine.TxSignature(publicKeyScript, tx, inputIndex, hashType);
            var txSignatureHash = SHA256Static.ComputeDoubleHash(txSignature);

            //Debug.WriteLine("Signing Tx:       {0}".Format2(txSignature.ToHexDataString()));
            //Debug.WriteLine("Signing Tx  Hash: {0}".Format2(txSignatureHash.ToHexDataString()));

            var signer = new ECDsaSigner();
            signer.Init(forSigning: true, parameters: privateKey);
            var signature = signer.GenerateSignature(txSignatureHash);
            var r = signature[0];
            var s = signature[1];

            byte[] sigEncoded;
            using (var stream = new MemoryStream())
            {
                using (var asn1Stream = new Asn1OutputStream(stream))
                {
                    asn1Stream.WriteObject(new DerSequence(new DerInteger(r), new DerInteger(s)));
                }

                sigEncoded = stream.ToArray().Concat(hashType);
            }

            //Debug.WriteLine("Sig R:       {0}".Format2(r.ToHexNumberStringUnsigned()));
            //Debug.WriteLine("Sig S:       {0}".Format2(s.ToHexNumberStringUnsigned()));
            //Debug.WriteLine("Sig Encoded: {0}".Format2(sigEncoded.ToHexDataString()));

            using (var privateKeyScript = new ScriptBuilder())
            {
                privateKeyScript.WritePushData(sigEncoded);
                privateKeyScript.WritePushData(publicAddress);
                //Debug.WriteLine("Private Script: {0}".Format2(privateKeyScript.GetScript().ToHexDataString()));

                return privateKeyScript.GetScript();
            }
        }

        public Transaction CreateCoinbaseTransaction(ECPublicKeyParameters publicKey, byte[] coinbase)
        {
            var tx = new Transaction
            (
                version: 1,
                inputs: ImmutableArray.Create
                (
                    new TxInput
                    (
                        previousTxOutputKey: new TxOutputKey
                        (
                            txHash: UInt256.Zero,
                            txOutputIndex: 0
                        ),
                        scriptSignature: coinbase,
                        sequence: 0
                    )
                ),
                outputs: ImmutableArray.Create
                (
                    new TxOutput
                    (
                        value: 50L * (100 * 1000 * 1000),
                        scriptPublicKey: CreatePublicKeyScript(publicKey)
                    )
                ),
                lockTime: 0
            );

            return tx;
        }

        public Transaction CreateSpendTransaction(Transaction prevTx, int prevInputIndex, byte hashType, UInt64 value, ECPrivateKeyParameters fromPrivateKey, ECPublicKeyParameters fromPublicKey, ECPublicKeyParameters toPublicKey)
        {
            var tx = new Transaction
            (
                version: 1,
                inputs: ImmutableArray.Create
                (
                    new TxInput
                    (
                        previousTxOutputKey: new TxOutputKey
                        (
                            txHash: prevTx.Hash,
                            txOutputIndex: (UInt32)prevInputIndex
                        ),
                        scriptSignature: new byte[0],
                        sequence: 0
                    )
                ),
                outputs: ImmutableArray.Create
                (
                    new TxOutput
                    (
                        value: value,
                        scriptPublicKey: CreatePublicKeyScript(toPublicKey)
                    )
                ),
                lockTime: 0
            );

            // sign the transaction
            var scriptSignature = CreatePrivateKeyScript(tx, 0, hashType, fromPrivateKey, fromPublicKey);

            // add the signature script to the transaction
            tx = tx.With(Inputs: ImmutableArray.Create(tx.Inputs[0].With(scriptSignature: scriptSignature)));

            return tx;
        }
    }
}
