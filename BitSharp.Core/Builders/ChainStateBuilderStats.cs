using BitSharp.Common;
using System;
using System.Diagnostics;
using System.Text;

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
        internal readonly DurationMeasure txesDecodeDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
        internal readonly DurationMeasure lookAheadDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
        internal readonly DurationMeasure calculateUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
        internal readonly DurationMeasure applyUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
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
            this.txesDecodeDurationMeasure.Dispose();
            this.lookAheadDurationMeasure.Dispose();
            this.calculateUtxoDurationMeasure.Dispose();
            this.applyUtxoDurationMeasure.Dispose();
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

            var durationFormatted = $"{Math.Floor(duration.TotalHours):#,#00}:{duration:mm':'ss}";

            statString.AppendLine($"Chain State Builder Stats");
            statString.AppendLine($"-------------------------");
            statString.AppendLine($"Height:           {Height,15:N0}");
            statString.AppendLine($"Duration:         {durationFormatted,15}");
            statString.AppendLine($"-------------------------");
            statString.AppendLine($"Blocks Rate:      {blockRateMeasure.GetAverage(),15:N0}/s");
            statString.AppendLine($"Tx Rate:          {txRateMeasure.GetAverage(),15:N0}/s");
            statString.AppendLine($"Input Rate:       {inputRateMeasure.GetAverage(),15:N0}/s");
            statString.AppendLine($"-------------------------");
            statString.AppendLine($"Txes per block:   {txesPerBlockMeasure.GetAverage(),15:N0}");
            statString.AppendLine($"Inputs per block: {inputsPerBlockMeasure.GetAverage(),15:N0}");
            statString.AppendLine($"-------------------------");
            statString.AppendLine($"Processed Txes:   {TotalTxCount,15:N0}");
            statString.AppendLine($"Processed Inputs: {TotalInputCount,15:N0}");
            statString.AppendLine($"Utx Size:         {UnspentTxCount,15:N0}");
            statString.AppendLine($"Utxo Size:        {UnspentOutputCount,15:N0}");
            statString.AppendLine($"-------------------------");

            var txesReadDuration = txesReadDurationMeasure.GetAverage();
            var txesDecodeDuration = txesDecodeDurationMeasure.GetAverage();
            var lookAheadDuration = lookAheadDurationMeasure.GetAverage();
            var calculateUtxoDuration = calculateUtxoDurationMeasure.GetAverage();
            var applyUtxoDuration = applyUtxoDurationMeasure.GetAverage();
            var validateDuration = validateDurationMeasure.GetAverage();
            var commitUtxoDuration = commitUtxoDurationMeasure.GetAverage();
            var addBlockDuration = addBlockDurationMeasure.GetAverage();

            statString.AppendLine(GetPipelineStat("Block Txes Read", txesReadDuration, TimeSpan.Zero));
            statString.AppendLine(GetPipelineStat("Block Txes Decode", txesDecodeDuration, txesReadDuration));
            statString.AppendLine(GetPipelineStat("UTXO Look-ahead", lookAheadDuration, txesDecodeDuration));
            statString.AppendLine(GetPipelineStat("UTXO Calculation", calculateUtxoDuration, lookAheadDuration));
            statString.AppendLine(GetPipelineStat("UTXO Application", applyUtxoDuration, calculateUtxoDuration));
            statString.AppendLine(GetPipelineStat("Block Validation", validateDuration, calculateUtxoDuration));
            statString.AppendLine(GetPipelineStat("UTXO Commit", commitUtxoDuration,
                TimeSpan.FromTicks(Math.Max(applyUtxoDuration.Ticks, validateDuration.Ticks))));
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
