#if SECP256K1_DLL
using Secp256k1;
#endif

using System;
using System.Diagnostics;
using System.Linq;
using System.IO;
using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.ExtensionMethods;
using System.Collections.Immutable;
using NLog;
using BitSharp.Core.Domain;

namespace BitSharp.Core.Script
{
    public class ScriptEngine
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly bool ignoreSignatures;

        public ScriptEngine()
        {
            this.ignoreSignatures = false;
        }

        //TODO
        internal ScriptEngine(bool ignoreSignatures)
        {
            this.ignoreSignatures = ignoreSignatures;
        }

        public bool VerifyScript(UInt256 blockHash, int txIndex, byte[] scriptPubKey, Transaction tx, int inputIndex, byte[] script)
        {
            if (this.logger.IsTraceEnabled)
                this.logger.Trace(
@"++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Verifying script for block {0}, transaction {1}, input {2}
{3}
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
                    , blockHash, txIndex, inputIndex, script.ToArray().ToHexDataString());

            Stack stack, altStack;
            if (
                ExecuteOps(scriptPubKey, tx, inputIndex, script, out stack, out altStack)
                && stack.Count == 1 && altStack.Count == 0)
            {
                var success = stack.PeekBool(); //TODO Pop? does it matter?

                // Additional validation for spend-to-script-hash transactions:
                //TODO

                return success;
            }
            else
            {
                return false;
            }
        }

        private bool ExecuteOps(byte[] scriptPubKey, Transaction tx, int inputIndex, byte[] script, out Stack stack, out Stack altStack)
        {
            stack = new Stack();
            altStack = new Stack();

            using (var scriptStream = new MemoryStream(script))
            using (var opReader = new BinaryReader(scriptStream))
            {
                while (opReader.BaseStream.Position < script.Length)
                {
                    var opByte = opReader.ReadByte();
                    var op = (ScriptOp)Enum.ToObject(typeof(ScriptOp), opByte);

                    if (this.logger.IsTraceEnabled)
                        this.logger.Trace("Executing {0} with stack count: {1}", OpName(opByte), stack.Count);

                    switch (op)
                    {
                        // Constants
                        case ScriptOp.OP_PUSHDATA1:
                            {
                                if (opReader.BaseStream.Position + 1 >= script.Length)
                                    return false;

                                var length = opReader.ReadByte();
                                stack.PushBytes(opReader.ReadBytes(length));
                            }
                            break;

                        case ScriptOp.OP_PUSHDATA2:
                            {
                                if (opReader.BaseStream.Position + 2 >= script.Length)
                                    return false;

                                var length = opReader.ReadUInt16();
                                stack.PushBytes(opReader.ReadBytes(length));
                            }
                            break;

                        case ScriptOp.OP_PUSHDATA4:
                            {
                                if (opReader.BaseStream.Position + 4 >= script.Length)
                                    return false;

                                var length = opReader.ReadUInt32();
                                stack.PushBytes(opReader.ReadBytes(length.ToIntChecked()));
                            }
                            break;

                        // Flow control
                        case ScriptOp.OP_NOP:
                            {
                            }
                            break;

                        // Stack
                        case ScriptOp.OP_DROP:
                            {
                                if (stack.Count < 1)
                                    return false;

                                var value = stack.PopBytes();

                                if (this.logger.IsTraceEnabled)
                                    this.logger.Trace("{0} dropped {1}", OpName(opByte), value);
                            }
                            break;

                        case ScriptOp.OP_DUP:
                            {
                                if (stack.Count < 1)
                                    return false;

                                var value = stack.PeekBytes();
                                stack.PushBytes(value);

                                if (this.logger.IsTraceEnabled)
                                    this.logger.Trace("{0} duplicated {2}", OpName(opByte), value);
                            }
                            break;

                        // Splice

                        // Bitwise logic
                        case ScriptOp.OP_EQUAL:
                        case ScriptOp.OP_EQUALVERIFY:
                            {
                                if (stack.Count < 2)
                                    return false;

                                var value1 = stack.PopBytes();
                                var value2 = stack.PopBytes();

                                var result = value1.SequenceEqual(value2);
                                stack.PushBool(result);

                                if (this.logger.IsTraceEnabled)
                                    this.logger.Trace(
@"{0} compared values:
    value1: {1}
    value2: {2}
    result: {3}", OpName(opByte), value1, value2, result);

                                if (op == ScriptOp.OP_EQUALVERIFY)
                                {
                                    if (result)
                                        stack.PopBool();
                                    else
                                        return false;
                                }
                            }
                            break;

                        // Arithmetic
                        // Note: Arithmetic inputs are limited to signed 32-bit integers, but may overflow their output.

                        // Crypto
                        case ScriptOp.OP_SHA256:
                            {
                                if (stack.Count < 1)
                                    return false;

                                var value = stack.PopBytes().ToArray();

                                var hash = SHA256Static.ComputeHash(value);
                                stack.PushBytes(hash);

                                if (this.logger.IsTraceEnabled)
                                    this.logger.Trace(
@"{0} hashed value:
    value:  {1}
    hash:   {2}", OpName(opByte), value, hash);
                            }
                            break;

                        case ScriptOp.OP_HASH160:
                            {
                                if (stack.Count < 1)
                                    return false;

                                var value = stack.PopBytes().ToArray();

                                var hash = RIPEMD160Static.ComputeHash(SHA256Static.ComputeHash(value));
                                stack.PushBytes(hash);

                                if (this.logger.IsTraceEnabled)
                                    this.logger.Trace(
@"{0} hashed value:
    value:  {1}
    hash:   {2}", OpName(opByte), value, hash);
                            }
                            break;

                        case ScriptOp.OP_CHECKSIG:
                        case ScriptOp.OP_CHECKSIGVERIFY:
                            {
                                if (stack.Count < 2)
                                    return false;

                                var pubKey = stack.PopBytes().ToArray();
                                var sig = stack.PopBytes().ToArray();

                                var startTime = DateTime.UtcNow;

                                byte hashType; byte[] txSignature, txSignatureHash;
                                var result = VerifySignature(scriptPubKey, tx, sig, pubKey, inputIndex, out hashType, out txSignature, out txSignatureHash);
                                stack.PushBool(result);

                                var finishTime = DateTime.UtcNow;

                                if (this.logger.IsTraceEnabled)
                                    this.logger.Trace(
@"{0} executed in {9} ms:
    tx:                 {1}
    inputIndex:         {2}
    pubKey:             {3}
    sig:                {4}
    hashType:           {5}
    txSignature:        {6}
    txSignatureHash:    {7}
    result:             {8}", OpName(opByte), new byte[0] /*tx.ToRawBytes()*/, inputIndex, pubKey, sig, hashType, txSignature, txSignatureHash, result, (finishTime - startTime).TotalMilliseconds.ToString("0"));

                                if (op == ScriptOp.OP_CHECKSIGVERIFY)
                                {
                                    if (result)
                                        stack.PopBool();
                                    else
                                        return false;
                                }
                            }
                            break;

                        // Pseudo-words
                        // These words are used internally for assisting with transaction matching. They are invalid if used in actual scripts.

                        // Reserved words
                        // Any opcode not assigned is also reserved. Using an unassigned opcode makes the transaction invalid.

                        default:
                            //OP_PUSHBYTES1-75
                            if (opByte >= (int)ScriptOp.OP_PUSHBYTES1 && opByte <= (int)ScriptOp.OP_PUSHBYTES75)
                            {
                                stack.PushBytes(opReader.ReadBytes(opByte));

                                if (this.logger.IsTraceEnabled)
                                    this.logger.Trace("{0} loaded {1} bytes onto the stack: {2}", OpName(opByte), opByte, stack.PeekBytes());
                            }
                            // Unknown op
                            else
                            {
                                var message = string.Format("Invalid operation in tx {0} input {1}: {2} {3}", tx.Hash.ToHexNumberString(), inputIndex, new[] { opByte }.ToHexNumberString(), OpName(opByte));
                                //this.logger.Warn(message);
                                throw new Exception(message);
                            }
                            break;
                    }

                    if (this.logger.IsTraceEnabled)
                        this.logger.Trace(new string('-', 80));
                }
            }

            // TODO verify no if/else blocks left over

            // TODO not entirely sure what default return should be
            return true;
        }

        public bool VerifySignature(byte[] scriptPubKey, Transaction tx, byte[] sig, byte[] pubKey, int inputIndex, out byte hashType, out byte[] txSignature, out byte[] txSignatureHash)
        {
            // get the 1-byte hashType off the end of sig
            hashType = sig[sig.Length - 1];

            // get the DER encoded portion of sig, which is everything except the last byte (the last byte being hashType)
            var sigDER = sig.Take(sig.Length - 1).ToArray();

            // get the simplified/signing version of the transaction
            txSignature = TxSignature(scriptPubKey, tx, inputIndex, hashType);

            // get the hash of the simplified/signing version of the transaction
            txSignatureHash = SHA256Static.ComputeDoubleHash(txSignature);

            if (this.ignoreSignatures)
            {
                return true;
            }
            else
            {
#if SECP256K1_DLL
                // verify that signature is valid for pubKey and the simplified/signing transaction's hash
                return Signatures.Verify(txSignatureHash, sigDER, pubKey) == Signatures.VerifyResult.Verified;
#else
                return true;
#endif
            }
        }

        public byte[] TxSignature(byte[] scriptPubKey, Transaction tx, int inputIndex, byte hashType)
        {
            ///TODO
            Debug.Assert(inputIndex < tx.Inputs.Length);

            // Blank out other inputs' signatures
            var empty = new byte[0];
            var newInputs = ImmutableArray.CreateBuilder<TxInput>(tx.Inputs.Length);
            for (var i = 0; i < tx.Inputs.Length; i++)
            {
                var oldInput = tx.Inputs[i];
                var newInput = oldInput.With(scriptSignature: i == inputIndex ? scriptPubKey : empty);
                newInputs.Add(newInput);
            }

            //// Blank out some of the outputs
            //if ((hashType & 0x1F) == (int)ScriptHashType.SIGHASH_NONE)
            //{
            //    //TODO
            //    Debug.Assert(false);

            //    // Wildcard payee

            //    // Let the others update at will
            //}
            //else if ((hashType & 0x1F) == (int)ScriptHashType.SIGHASH_SINGLE)
            //{
            //    //TODO
            //    Debug.Assert(false);

            //    // Only lock-in the txout payee at same index as txin

            //    // Let the others update at will
            //}

            //// Blank out other inputs completely, not recommended for open transactions
            //if ((hashType & 0x80) == (int)ScriptHashType.SIGHASH_ANYONECANPAY)
            //{
            //    //TODO
            //    Debug.Assert(false);
            //}

            // create simplified transaction
            var newTx = tx.With(Inputs: newInputs.ToImmutable());

            // return wire-encoded simplified transaction with the 4-byte hashType tacked onto the end
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                writer.WriteBytes(DataEncoder.EncodeTransaction(newTx));
                writer.WriteUInt32(hashType);

                return stream.ToArray();
            }
        }

        private string OpName(byte op)
        {
            if (op >= (int)ScriptOp.OP_PUSHBYTES1 && op <= (int)ScriptOp.OP_PUSHBYTES75)
            {
                return string.Format("OP_PUSHBYTES{0}", op);
            }
            else
            {
                return Enum.ToObject(typeof(ScriptOp), op).ToString();
            }
        }
    }
}
