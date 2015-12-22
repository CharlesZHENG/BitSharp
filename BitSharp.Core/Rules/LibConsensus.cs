using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Rules
{
    // See: https://github.com/libbitcoin/libbitcoin-consensus/

    public class LibConsensus
    {
        [DllImport("libbitcoin-consensus.dll",
            EntryPoint = "?verify_script@consensus@libbitcoin@@YA?AW4verify_result_type@12@PEBE_K01II@Z",
            CallingConvention = CallingConvention.Cdecl)]
        private static extern verify_result_type verify_script(
            [In] byte[] transaction,
            [In] UIntPtr transaction_size,
            [In] byte[] prevout_script,
            [In] UIntPtr prevout_script_size,
            [In] uint tx_input_index,
            [In] verify_flags_type flags);

        public static bool VerifyScript(ImmutableArray<byte> txBytes, ImmutableArray<byte> prevTxOutputPublicScriptBytes, int inputIndex)
        {
            //TODO
            if (!Environment.Is64BitProcess)
                return true;

            //TODO
            var flags = verify_flags_type.verify_flags_none;

            return verify_script(txBytes.ToArray(), (UIntPtr)txBytes.Length, prevTxOutputPublicScriptBytes.ToArray(), (UIntPtr)prevTxOutputPublicScriptBytes.Length, (uint)inputIndex, flags)
                == verify_result_type.verify_result_eval_true;
        }
    }

    public enum verify_result_type : uint
    {
        // Logical result
        verify_result_eval_false = 0,
        verify_result_eval_true,

        // Max size errors
        verify_result_script_size,
        verify_result_push_size,
        verify_result_op_count,
        verify_result_stack_size,
        verify_result_sig_count,
        verify_result_pubkey_count,

        // Failed verify operations
        verify_result_verify,
        verify_result_equalverify,
        verify_result_checkmultisigverify,
        verify_result_checksigverify,
        verify_result_numequalverify,

        // Logical/Format/Canonical errors
        verify_result_bad_opcode,
        verify_result_disabled_opcode,
        verify_result_invalid_stack_operation,
        verify_result_invalid_altstack_operation,
        verify_result_unbalanced_conditional,

        // BIP62 errors
        verify_result_sig_hashtype,
        verify_result_sig_der,
        verify_result_minimaldata,
        verify_result_sig_pushonly,
        verify_result_sig_high_s,
        verify_result_sig_nulldummy,
        verify_result_pubkeytype,
        verify_result_cleanstack,

        // Softfork safeness
        verify_result_discourage_upgradable_nops,

        // Other
        verify_result_op_return,
        verify_result_unknown_error,

        // augmention codes for tx deserialization
        verify_result_tx_invalid,
        verify_result_tx_size_invalid,
        verify_result_tx_input_invalid,

        // BIP65 errors
        verify_result_negative_locktime,
        verify_result_unsatisfied_locktime
    }

    [Flags]
    public enum verify_flags_type : uint
    {
        /**
         * Set no flags.
         */
        verify_flags_none = 0,

        /**
         * Evaluate P2SH subscripts (softfork safe, BIP16).
         */
        verify_flags_p2sh = (1U << 0),

        /**
         * Passing a non-strict-DER signature or one with undefined hashtype to a
         * checksig operation causes script failure. Evaluating a pubkey that is 
         * not (0x04 + 64 bytes) or (0x02 or 0x03 + 32 bytes) by checksig causes 
         * script failure. (softfork safe, but not used or intended as a consensus
         * rule).
         */
        verify_flags_strictenc = (1U << 1),

        /**
         * Passing a non-strict-DER signature to a checksig operation causes script
         * failure (softfork safe, BIP62 rule 1).
         */
        verify_flags_dersig = (1U << 2),

        /**
         * Passing a non-strict-DER signature or one with S > order/2 to a checksig
         * operation causes script failure
         * (softfork safe, BIP62 rule 5).
         */
        verify_flags_low_s = (1U << 3),

        /**
         * verify dummy stack item consumed by CHECKMULTISIG is of zero-length
         * (softfork safe, BIP62 rule 7).
         */
        verify_flags_nulldummy = (1U << 4),

        /**
         * Using a non-push operator in the scriptSig causes script failure
         * (softfork safe, BIP62 rule 2).
         */
        verify_flags_sigpushonly = (1U << 5),

        /**
         * Require minimal encodings for all push operations (OP_0... OP_16,
         * OP_1NEGATE where possible, direct pushes up to 75 bytes, OP_PUSHDATA
         * up to 255 bytes, OP_PUSHDATA2 for anything larger). Evaluating any other
         * push causes the script to fail (BIP62 rule 3). In addition, whenever a
         * stack element is interpreted as a number, it must be of minimal length
         * (BIP62 rule 4).(softfork safe)
         */
        verify_flags_minimaldata = (1U << 6),

        /**
         * Discourage use of NOPs reserved for upgrades (NOP1,3-10)
         * Provided so that nodes can avoid accepting or mining transactions
         * containing executed NOP's whose meaning may change after a soft-fork,
         * thus rendering the script invalid; with this flag set executing
         * discouraged NOPs fails the script. This verification flag will never be
         * a mandatory flag applied to scripts in a block. NOPs that are not
         * executed, e.g.  within an unexecuted IF ENDIF block, are *not* rejected.
         */
        verify_flags_discourage_upgradable_nops = (1U << 7),

        /**
         * Require that only a single stack element remains after evaluation. This
         * changes the success criterion from "At least one stack element must
         * remain, and when interpreted as a boolean, it must be true" to "Exactly
         * one stack element must remain, and when interpreted as a boolean, it
         * must be true". (softfork safe, BIP62 rule 6)
         * Note: verify_flags_cleanstack must be used with verify_flags_p2sh.
         */
        verify_flags_cleanstack = (1U << 8),

        /**
         * Verify CHECKLOCKTIMEVERIFY, see BIP65 for details.
         */
        verify_flags_checklocktimeverify = (1U << 9)
    }
}
