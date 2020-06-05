const express  = require('express')
const path = require('path')
const app = express()

const CONTACTS = [
    {id:Date.now(), name:'Dkfsdf', value:'9879797', marked: false}
]

app.use(express.json())

app.get('/api/contacts',(req , res) => {
    setTimeout(() =>{
        res.status(200).json(CONTACTS)
    }, 2000)
    
})


app.post('/api/contacts',(req , res) => {
    const contact = {...req.body, id:Date.now(), marked: false}
    CONTACTS.push(contact)
    res.status(201).json(contact)
    
})

app.use(express.static(path.resolve(__dirname, 'client')))
app.get('/', (req , res) => {
    res.sendFile(__dirname, 'client', 'index.html')
})

app.listen(3000, () => console.log('Its have been started'))