using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    internal sealed class ChainStateBuilderStats : IDisposable
    {
        private static readonly TimeSpan sampleCutoff = TimeSpan.FromMinutes(5);
        private static readonly TimeSpan sampleResolution = TimeSpan.FromSeconds(5);

        internal Stopwatch durationStopwatch = Stopwatch.StartNew();

        public int Height { get; internal set; }
        public int TotalTxCount { get; internal set; }
        public int TotalInputCount { get; internal set; }
        public int UnspentTxCount { get; internal set; }
        public int UnspentOutputCount { get; internal set; }

        internal readonly RateMeasure blockRateMeasure = new RateMeasure(sampleCutoff, TimeSpan.FromSeconds(1));
        internal readonly RateMeasure txRateMeasure = new RateMeasure(sampleCutoff, TimeSpan.FromSeconds(1));
        internal readonly RateMeasure inputRateMeasure = new RateMeasure(sampleCutoff, TimeSpan.FromSeconds(1));

        internal readonly AverageMeasure txesPerBlockMeasure = new AverageMeasure(sampleCutoff, sampleResolution);
        internal readonly AverageMeasure inputsPerBlockMeasure = new AverageMeasure(sampleCutoff, sampleResolution);

        internal readonly DurationMeasure txesReadDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
        internal readonly DurationMeasure lookAheadDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
        internal readonly DurationMeasure calculateUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
        internal readonly DurationMeasure applyUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
        internal readonly DurationMeasure loadTxesDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
        internal readonly DurationMeasure validateDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
        internal readonly DurationMeasure commitUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
        internal readonly DurationMeasure addBlockDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);

        internal ChainStateBuilderStats() { }

        public void Dispose()
        {
            this.blockRateMeasure.Dispose();
            this.txRateMeasure.Dispose();
            this.inputRateMeasure.Dispose();

            this.txesPerBlockMeasure.Dispose();
            this.inputsPerBlockMeasure.Dispose();

            this.txesReadDurationMeasure.Dispose();
            this.lookAheadDurationMeasure.Dispose();
            this.calculateUtxoDurationMeasure.Dispose();
            this.applyUtxoDurationMeasure.Dispose();
            this.loadTxesDurationMeasure.Dispose();
            this.validateDurationMeasure.Dispose();
            this.commitUtxoDurationMeasure.Dispose();
            this.addBlockDurationMeasure.Dispose();
        }

        public override string ToString()
        {
            var statString = new StringBuilder();

            TimeSpan duration;
            lock (durationStopwatch)
                duration = durationStopwatch.Elapsed;

            statString.AppendLine("Chain State Builder Stats");
            statString.AppendLine("-------------------------");
            statString.AppendLine("Height:           {0,15:N0}".Format2(Height));
            statString.AppendLine("Duration:         {0,15}".Format2(
                "{0:#,#00}:{1:mm':'ss}".Format2(Math.Floor(duration.TotalHours), duration)));
            statString.AppendLine("-------------------------");
            statString.AppendLine("Blocks Rate:      {0,15:N0}/s".Format2(blockRateMeasure.GetAverage()));
            statString.AppendLine("Tx Rate:          {0,15:N0}/s".Format2(txRateMeasure.GetAverage()));
            statString.AppendLine("Input Rate:       {0,15:N0}/s".Format2(inputRateMeasure.GetAverage()));
            statString.AppendLine("-------------------------");
            statString.AppendLine("Txes per block:   {0,15:N0}".Format2(txesPerBlockMeasure.GetAverage()));
            statString.AppendLine("Inputs per block: {0,15:N0}".Format2(inputsPerBlockMeasure.GetAverage()));
            statString.AppendLine("-------------------------");
            statString.AppendLine("Processed Txes:   {0,15:N0}".Format2(TotalTxCount));
            statString.AppendLine("Processed Inputs: {0,15:N0}".Format2(TotalInputCount));
            statString.AppendLine("Utx Size:         {0,15:N0}".Format2(UnspentTxCount));
            statString.AppendLine("Utxo Size:        {0,15:N0}".Format2(UnspentOutputCount));
            statString.AppendLine("-------------------------");

            var texReadDuration = txesReadDurationMeasure.GetAverage();
            var lookAheadDuration = lookAheadDurationMeasure.GetAverage();
            var calculateUtxoDuration = calculateUtxoDurationMeasure.GetAverage();
            var applyUtxoDuration = applyUtxoDurationMeasure.GetAverage();
            var loadTxesDuration = loadTxesDurationMeasure.GetAverage();
            var validateDuration = validateDurationMeasure.GetAverage();
            var commitUtxoDuration = commitUtxoDurationMeasure.GetAverage();
            var addBlockDuration = addBlockDurationMeasure.GetAverage();

            statString.AppendLine(GetPipelineStat("Block Txes Read", texReadDuration, TimeSpan.Zero));
            statString.AppendLine(GetPipelineStat("UTXO Look-ahead", lookAheadDuration, texReadDuration));
            statString.AppendLine(GetPipelineStat("UTXO Calculation", calculateUtxoDuration, lookAheadDuration));
            statString.AppendLine(GetPipelineStat("UTXO Application", applyUtxoDuration, calculateUtxoDuration));
            statString.AppendLine(GetPipelineStat("Load Txes", loadTxesDuration, applyUtxoDuration));
            statString.AppendLine(GetPipelineStat("Block Validation", validateDuration, loadTxesDuration));
            statString.AppendLine(GetPipelineStat("UTXO Commit", commitUtxoDuration, validateDuration));
            statString.Append(GetPipelineStat("AddBlock Total", addBlockDuration, null));

            return statString.ToString();
        }

        private string GetPipelineStat(string name, TimeSpan duration, TimeSpan? prevDuration)
        {
            var format = "{0,-20} Completion: {1,12:N3}ms";

            TimeSpan delta;
            if (prevDuration != null)
            {
                format += ", Delta: {2,12:N3}ms";
                delta = duration - prevDuration.Value;
            }
            else
                delta = TimeSpan.Zero;

            return string.Format(format, name + ":", duration.TotalMilliseconds, delta.TotalMilliseconds);
        }
    }
}
