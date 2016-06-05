

namespace Org.Apache.Hadoop.Record.Compiler
{
	/// <summary>Code generator for "byte" type.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JByte : JType
	{
		internal class JavaByte : JType.JavaType
		{
			internal JavaByte(JByte _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.ByteTypeID";
			}

			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				cb.Append("if (" + l + "<1) {\n");
				cb.Append("throw new java.io.IOException(\"Byte is exactly 1 byte." + " Provided buffer is smaller.\");\n"
					);
				cb.Append("}\n");
				cb.Append(s + "++; " + l + "--;\n");
				cb.Append("}\n");
			}

			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				cb.Append("if (l1<1 || l2<1) {\n");
				cb.Append("throw new java.io.IOException(\"Byte is exactly 1 byte." + " Provided buffer is smaller.\");\n"
					);
				cb.Append("}\n");
				cb.Append("if (b1[s1] != b2[s2]) {\n");
				cb.Append("return (b1[s1]<b2[s2])?-1:0;\n");
				cb.Append("}\n");
				cb.Append("s1++; s2++; l1--; l2--;\n");
				cb.Append("}\n");
			}

			private readonly JByte _enclosing;
		}

		internal class CppByte : JType.CppType
		{
			internal CppByte(JByte _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_BYTE)";
			}

			private readonly JByte _enclosing;
		}

		public JByte()
		{
			SetJavaType(new JByte.JavaByte(this));
			SetCppType(new JByte.CppByte(this));
			SetCType(new JType.CType(this));
		}

		internal override string GetSignature()
		{
			return "b";
		}
	}
}
