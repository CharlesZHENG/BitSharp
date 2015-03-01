
namespace BitSharp.Node.Domain
{
    public class AlertPayload
    {
        public readonly string Payload;
        public readonly string Signature;

        public AlertPayload(string Payload, string Signature)
        {
            this.Payload = Payload;
            this.Signature = Signature;
        }

        public AlertPayload With(string Payload = null, string Signature = null)
        {
            return new AlertPayload
            (
                Payload ?? this.Payload,
                Signature ?? this.Signature
            );
        }
    }
}
