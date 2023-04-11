import requests
import streamlit as st
import pandas as pd
import base64
def get_user_history(user_id):

    response = requests.get(f'http://localhost:8000/get/{user_id}')
    data = response.json()
    df = pd.DataFrame(data, columns=['Products', 'Timestamp'])
    return df

def delete_user_history(user_id, product_id):

    response = requests.get(f'http://localhost:8000/delete/{user_id}/{product_id}')
    
    return  response.json()
 

def recommend(user_id):

    response = requests.get(f'http://localhost:8000/personalized/user-53')
    response = response.json()
    return response["Products"]

def bestsellers():
    return "Bestsellers"


def add_bg_from_local(image_file):
    with open(image_file, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    st.markdown(
    f"""
    <style>
    .stApp {{
        background-image: url(data:image/{"png"};base64,{encoded_string.decode()});
        background-size: cover
        opacity: -1.0;
    }}
    </style>
    """,
    unsafe_allow_html=True
    )

def streamlit_app():

    st.title('Recommendation App')


    # Define the style for the table
    table_style = """
            <style>
                .dataframe tbody tr:nth-child(even) {
                    background-color: #f2f2f2;
                }
                .dataframe th {
                    background-color: #ddd;
                    font-weight: bold;
                    text-align: center;
                }
                .dataframe td {
                    text-align: center;
                }
            </style>
    """


    page = st.sidebar.selectbox("Select a page", ["Home", "User Info"])

    if page == "Home":

       
        add_bg_from_local("home.png")


        st.markdown("<h4>Enter your name to get personalized recommendation</h4>", unsafe_allow_html=True)
        userid = st.text_input('')
        if st.button('Recommend'):

            data = recommend(userid)
            data = pd.DataFrame(data=data, columns=["Product","Category"])
            st.markdown(table_style, unsafe_allow_html=True)
            st.table(data)


        st.markdown("<h4>General Best Sellers Of Last Month</h4>", unsafe_allow_html=True)
        if st.button('Best Sellers'):

            bestsellers()

    


    elif page == "User Info":


        add_bg_from_local("user.png")

        # Display the input field for the user's ID
        st.markdown("<h4>Enter a user ID to fetch the browsing history:</h4>", unsafe_allow_html=True)
        user_id = st.text_input("", key="user_id")

        # Fetch the user's browsing history and display it in a table
        if st.button("Get Browsing History"):
            if user_id:
                data = get_user_history(user_id)
                if data is not None:
                    st.markdown(table_style, unsafe_allow_html=True)
                    st.table(data)
                else:
                    st.write("No browsing history found for user", user_id)
            else:
                st.warning("Please enter a user ID to fetch the browsing history.")



        # Display the input field for the user's ID
        st.markdown("<h4>Fill the fields to delete your browsing history<h4>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)


        with col1:
            user = st.text_input('Enter your username:')

        with col2:
            product = st.text_input('Enter the product name:')

        if st.button("Delete history!"):
                        

            response = delete_user_history(user, product)
            st.write(response)




if __name__ == '__main__':
    streamlit_app()