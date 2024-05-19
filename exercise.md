# Programming Exercise using PySpark

## Background:
A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

Since all the data in the datasets is fake and this is just an exercise, one can forego the issue of having the data stored along with the code in a code repository.


## Things to be aware:

- Use Python **3.8**
- Avoid using notebooks, like **Jupyter** for instance. While these are good for interactive work and/or prototyping in this case they shouldn't be used.
- There's no need to use classes, because the assignment is quite small and not very complex in what it does classes are unnecessary.
- Only use clients from the **United Kingdom** or the **Netherlands**.
- Remove personal identifiable information from the first dataset, **excluding emails**.
- Remove credit card number from the second dataset.
- Data should be joined using the **id** field.
- Rename the columns for the easier readability to the business users:

|Old name|New name|
|--|--|
|id|client_identifier|
|btc_a|bitcoin_address|
|cc_t|credit_card_type|

- The project should be stored in GitHub and you should only commit relevant files to the repo.
- Save the output in a **client_data** directory in the root directory of the project.
- Add a **README** file explaining on a high level what the application does.
- Application should receive three arguments, the paths to each of the dataset files and also the countries to filter as the client wants to reuse the code for other countries.
- Use **logging**.
- Create generic functions for filtering data and renaming.
Recommendation: Use the following package for Spark tests - https://github.com/MrPowers/chispa
- If possible, have different branches for different tasks that once completed are merged to the main branch. Follow the GitHub flow - https://guides.github.com/introduction/flow/.
- **Bonus** - If possible it should have an automated build pipeline using GitHub Actions - https://docs.github.com/en/actions - or Travis - https://www.travis-ci.com/ for instance.
- **Bonus** - If possible log to a file with a rotating policy.
- **Bonus** - Code should be able to be packaged into a source distribution file.
- **Bonus** - Requirements file should exist.
- **Bonus** - Document the code with docstrings as much as possible using the reStructuredText (reST) format.
