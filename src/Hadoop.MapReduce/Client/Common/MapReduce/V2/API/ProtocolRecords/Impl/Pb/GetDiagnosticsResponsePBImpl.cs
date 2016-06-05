using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetDiagnosticsResponsePBImpl : ProtoBase<MRServiceProtos.GetDiagnosticsResponseProto
		>, GetDiagnosticsResponse
	{
		internal MRServiceProtos.GetDiagnosticsResponseProto proto = MRServiceProtos.GetDiagnosticsResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetDiagnosticsResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private IList<string> diagnostics = null;

		public GetDiagnosticsResponsePBImpl()
		{
			builder = MRServiceProtos.GetDiagnosticsResponseProto.NewBuilder();
		}

		public GetDiagnosticsResponsePBImpl(MRServiceProtos.GetDiagnosticsResponseProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetDiagnosticsResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetDiagnosticsResponseProto)builder.
				Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.diagnostics != null)
			{
				AddDiagnosticsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRServiceProtos.GetDiagnosticsResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetDiagnosticsResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual IList<string> GetDiagnosticsList()
		{
			InitDiagnostics();
			return this.diagnostics;
		}

		public virtual string GetDiagnostics(int index)
		{
			InitDiagnostics();
			return this.diagnostics[index];
		}

		public virtual int GetDiagnosticsCount()
		{
			InitDiagnostics();
			return this.diagnostics.Count;
		}

		private void InitDiagnostics()
		{
			if (this.diagnostics != null)
			{
				return;
			}
			MRServiceProtos.GetDiagnosticsResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<string> list = p.GetDiagnosticsList();
			this.diagnostics = new AList<string>();
			foreach (string c in list)
			{
				this.diagnostics.AddItem(c);
			}
		}

		public virtual void AddAllDiagnostics(IList<string> diagnostics)
		{
			if (diagnostics == null)
			{
				return;
			}
			InitDiagnostics();
			Sharpen.Collections.AddAll(this.diagnostics, diagnostics);
		}

		private void AddDiagnosticsToProto()
		{
			MaybeInitBuilder();
			builder.ClearDiagnostics();
			if (diagnostics == null)
			{
				return;
			}
			builder.AddAllDiagnostics(diagnostics);
		}

		public virtual void AddDiagnostics(string diagnostics)
		{
			InitDiagnostics();
			this.diagnostics.AddItem(diagnostics);
		}

		public virtual void RemoveDiagnostics(int index)
		{
			InitDiagnostics();
			this.diagnostics.Remove(index);
		}

		public virtual void ClearDiagnostics()
		{
			InitDiagnostics();
			this.diagnostics.Clear();
		}
	}
}
