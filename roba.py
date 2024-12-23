
def initialize_s3():
    s3 = boto3.client(
        's3',
        st.secrets["AWS_ACCESS_KEY_ID"],
        st.secrets["AWS_SECRET_ACCESS_KEY"],
        st.secrets["AWS_REGION"]
    )
    return s3