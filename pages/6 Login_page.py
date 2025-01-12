import streamlit as st
import warnings
# Disabilita i warnings
warnings.filterwarnings("ignore")
# Inizializza lo stato della variabile 'Light' se non esiste
st.set_page_config(layout="wide")

with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.logo("assets/Logo.svg",size="large")


if 'Light' not in st.session_state:
    st.session_state['Light'] = 'red'

# Se 'Light' è rosso, mostra il form di login
if st.session_state['Light'] == 'red':
    #st.set_page_config(initial_sidebar_state="collapsed")  # Collassa la sidebar inizialmente

    st.title("Login Todis - Via Castel Porziano")
    st.write(" ")

    user = st.text_input(label="Username", value="", key="user")
    password = st.text_input(label="Password", value="", key="password",type="password")

    if st.button('Login'):
        if user == "todis@predictai.it" and password == "todis_2025":
            st.session_state['Light'] = 'green'  # Cambia lo stato a verde
            st.rerun()  # Ricarica la pagina per applicare il cambiamento
        else:
            st.error("Username o Password non corretti!")
else:
    #st.set_page_config(initial_sidebar_state="expanded")  # Espandi la sidebar quando l'accesso è riuscito
    st.success("Hai effettuato l'accesso come Todis - Via Castel Porziano!")