

namespace Org.Apache.Hadoop.Record.Compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JFloat : JType
	{
		internal class JavaFloat : JType.JavaType
		{
			internal JavaFloat(JFloat _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.FloatTypeID";
			}

			internal override void GenHashCode(CodeBuffer cb, string fname)
			{
				cb.Append(Consts.RioPrefix + "ret = Float.floatToIntBits(" + fname + ");\n");
			}

			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				cb.Append("if (" + l + "<4) {\n");
				cb.Append("throw new java.io.IOException(\"Float is exactly 4 bytes." + " Provided buffer is smaller.\");\n"
					);
				cb.Append("}\n");
				cb.Append(s + "+=4; " + l + "-=4;\n");
				cb.Append("}\n");
			}

			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				cb.Append("if (l1<4 || l2<4) {\n");
				cb.Append("throw new java.io.IOException(\"Float is exactly 4 bytes." + " Provided buffer is smaller.\");\n"
					);
				cb.Append("}\n");
				cb.Append("float f1 = org.apache.hadoop.record.Utils.readFloat(b1, s1);\n");
				cb.Append("float f2 = org.apache.hadoop.record.Utils.readFloat(b2, s2);\n");
				cb.Append("if (f1 != f2) {\n");
				cb.Append("return ((f1-f2) < 0) ? -1 : 0;\n");
				cb.Append("}\n");
				cb.Append("s1+=4; s2+=4; l1-=4; l2-=4;\n");
				cb.Append("}\n");
			}

			private readonly JFloat _enclosing;
		}

		internal class CppFloat : JType.CppType
		{
			internal CppFloat(JFloat _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_FLOAT)";
			}

			private readonly JFloat _enclosing;
		}

		/// <summary>Creates a new instance of JFloat</summary>
		public JFloat()
		{
			SetJavaType(new JFloat.JavaFloat(this));
			SetCppType(new JFloat.CppFloat(this));
			SetCType(new JType.CType(this));
		}

		internal override string GetSignature()
		{
			return "f";
		}
	}
}
