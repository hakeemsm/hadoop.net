using Sharpen;

namespace Org.Apache.Hadoop.Record.Compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JDouble : JType
	{
		internal class JavaDouble : JType.JavaType
		{
			internal JavaDouble(JDouble _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.DoubleTypeID";
			}

			internal override void GenHashCode(CodeBuffer cb, string fname)
			{
				string tmp = "Double.doubleToLongBits(" + fname + ")";
				cb.Append(Consts.RioPrefix + "ret = (int)(" + tmp + "^(" + tmp + ">>>32));\n");
			}

			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				cb.Append("if (" + l + "<8) {\n");
				cb.Append("throw new java.io.IOException(\"Double is exactly 8 bytes." + " Provided buffer is smaller.\");\n"
					);
				cb.Append("}\n");
				cb.Append(s + "+=8; " + l + "-=8;\n");
				cb.Append("}\n");
			}

			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				cb.Append("if (l1<8 || l2<8) {\n");
				cb.Append("throw new java.io.IOException(\"Double is exactly 8 bytes." + " Provided buffer is smaller.\");\n"
					);
				cb.Append("}\n");
				cb.Append("double d1 = org.apache.hadoop.record.Utils.readDouble(b1, s1);\n");
				cb.Append("double d2 = org.apache.hadoop.record.Utils.readDouble(b2, s2);\n");
				cb.Append("if (d1 != d2) {\n");
				cb.Append("return ((d1-d2) < 0) ? -1 : 0;\n");
				cb.Append("}\n");
				cb.Append("s1+=8; s2+=8; l1-=8; l2-=8;\n");
				cb.Append("}\n");
			}

			private readonly JDouble _enclosing;
		}

		internal class CppDouble : JType.CppType
		{
			internal CppDouble(JDouble _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_DOUBLE)";
			}

			private readonly JDouble _enclosing;
		}

		/// <summary>Creates a new instance of JDouble</summary>
		public JDouble()
		{
			SetJavaType(new JDouble.JavaDouble(this));
			SetCppType(new JDouble.CppDouble(this));
			SetCType(new JType.CType(this));
		}

		internal override string GetSignature()
		{
			return "d";
		}
	}
}
