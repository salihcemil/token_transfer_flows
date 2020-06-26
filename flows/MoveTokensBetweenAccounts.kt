package com.t20worldcup.flows

import co.paralleluniverse.fibers.Suspendable
import com.r3.corda.lib.accounts.workflows.accountService
import com.r3.corda.lib.accounts.workflows.flows.RequestKeyForAccount
import com.r3.corda.lib.ci.workflows.SyncKeyMappingFlow
import com.r3.corda.lib.ci.workflows.SyncKeyMappingFlowHandler
import com.r3.corda.lib.tokens.contracts.states.FungibleToken
import com.r3.corda.lib.tokens.contracts.states.NonFungibleToken
import com.r3.corda.lib.tokens.contracts.types.TokenPointer
import com.r3.corda.lib.tokens.contracts.types.TokenType
import com.r3.corda.lib.tokens.money.FiatCurrency.Companion.getInstance
import com.r3.corda.lib.tokens.selection.TokenQueryBy
import com.r3.corda.lib.tokens.selection.database.config.MAX_RETRIES_DEFAULT
import com.r3.corda.lib.tokens.selection.database.config.PAGE_SIZE_DEFAULT
import com.r3.corda.lib.tokens.selection.database.config.RETRY_CAP_DEFAULT
import com.r3.corda.lib.tokens.selection.database.config.RETRY_SLEEP_DEFAULT
import com.r3.corda.lib.tokens.selection.database.selector.DatabaseTokenSelection
import com.r3.corda.lib.tokens.workflows.flows.move.addMoveNonFungibleTokens
import com.r3.corda.lib.tokens.workflows.flows.move.addMoveTokens
import com.r3.corda.lib.tokens.workflows.utilities.heldTokenAmountCriteria
import com.r3.corda.lib.tokens.workflows.utilities.sumTokenCriteria
import com.t20worldcup.states.T20CricketTicketState
import net.corda.core.contracts.Amount
import net.corda.core.contracts.CommandData
import net.corda.core.contracts.CommandWithParties
import net.corda.core.contracts.StateAndRef
import net.corda.core.flows.*
import net.corda.core.identity.AbstractParty
import net.corda.core.node.services.Vault
import net.corda.core.node.services.queryBy
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.node.services.vault.QueryCriteria.LinearStateQueryCriteria
import net.corda.core.node.services.vault.QueryCriteria.VaultQueryCriteria
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.ProgressTracker.Step
import net.corda.core.utilities.unwrap
import java.security.PublicKey
import java.util.*
import kotlin.math.sign


// *********
// * Flows *
// *********
@InitiatingFlow
@StartableByRPC
class MoveTokensBetweenAccounts(private val senderAccountName:String,
        private val receiverAccountName:String,
        private val costOfTicket: Long,
        private val currency: String) : FlowLogic<String>(){
    @Suspendable
    override fun call():String {

        //get sender info and account
        val senderInfo = accountService.accountInfo(senderAccountName)[0].state.data
        val senderAcct = subFlow(RequestKeyForAccount(senderInfo))

        //get receiver info and account
        val receiverInfo = accountService.accountInfo(receiverAccountName).single().state.data
        val receiverAcct = subFlow(RequestKeyForAccount(receiverInfo))

        //sender will create generate a move tokens state and send this state with new holder(seller) to receiver
        val amount = Amount(costOfTicket, getInstance(currency))

        val receiverSession = initiateFlow(receiverInfo.host)

        //send uuid, buyer,seller account name to seller
        receiverSession.send(senderAccountName)
        receiverSession.send(receiverAccountName)

        //sender Query for token balance.
        val queryCriteria = heldTokenAmountCriteria(getInstance(currency), senderAcct).and(sumTokenCriteria())

        val sum = serviceHub.vaultService.queryBy(FungibleToken::class.java, queryCriteria).component5()
        if (sum.size == 0) throw FlowException("$senderAccountName has 0 token balance. Please ask the Bank to issue some cash.") else {
            val tokenBalance = sum[0] as Long
            if (tokenBalance < costOfTicket) throw FlowException("Available token balance of $senderAccountName is less than the cost of the ticket. Please ask the Bank to issue some cash if you wish to buy the ticket ")
        }


        //the tokens to move to new account which is the seller account
        val partyAndAmount:Pair<AbstractParty, Amount<TokenType>> = Pair(receiverAcct, amount)

        //let's use the DatabaseTokenSelection to get the tokens from the db
        val tokenSelection = DatabaseTokenSelection(serviceHub, MAX_RETRIES_DEFAULT,
                RETRY_SLEEP_DEFAULT, RETRY_CAP_DEFAULT, PAGE_SIZE_DEFAULT)

        //call generateMove which gives us 2 stateandrefs with tokens having new owner as seller.
        val inputsAndOutputs = tokenSelection
                .generateMove(Arrays.asList(partyAndAmount), senderAcct, TokenQueryBy(), runId.uuid)

        //send the generated inputsAndOutputs to the seller
        subFlow(SendStateAndRefFlow(receiverSession, inputsAndOutputs.first))
        receiverSession.send(inputsAndOutputs.second)

        //sync following keys with seller - buyeraccounts, selleraccounts which we generated above using RequestKeyForAccount, and IMP: also share the anonymouse keys
        //created by the above token move method for the holder.
        val signers: MutableList<AbstractParty> = ArrayList()
        signers.add(senderAcct)
        signers.add(receiverAcct)

        val inputs = inputsAndOutputs.first
        for ((state) in inputs) {
            signers.add(state.data.holder)
        }

        //Sync our associated keys with the conterparties.
        subFlow(SyncKeyMappingFlow(receiverSession, signers))

        //this is the handler for synckeymapping called by seller. seller must also have created some keys not known to us - buyer
        subFlow(SyncKeyMappingFlowHandler(receiverSession))

        //recieve the data from counter session in tx formatt.
        subFlow(object : SignTransactionFlow(receiverSession) {
            @Throws(FlowException::class)
            override fun checkTransaction(stx: SignedTransaction) {
                // Custom Logic to validate transaction.
            }
        })

        val stx = subFlow(ReceiveFinalityFlow(receiverSession))

        return ("The ticket is sold to $receiverAccountName"+ "\ntxID: "+stx.id)
    }
}

@InitiatedBy(MoveTokensBetweenAccounts::class)
class MoveTokensBetweenAccountsResponder(val otherSide: FlowSession) : FlowLogic<SignedTransaction>() {
    @Suspendable
    override fun call():SignedTransaction {
        //get all the details from the seller
        //val tokenId: String = otherSide.receive(String::class.java).unwrap { it }
        val senderAccountName: String = otherSide.receive(String::class.java).unwrap { it }
        val receiverAccountName: String = otherSide.receive(String::class.java).unwrap{ it }

        val inputs = subFlow(ReceiveStateAndRefFlow<FungibleToken>(otherSide))
        val moneyReceived: List<FungibleToken> = otherSide.receive(List::class.java).unwrap{ it } as List<FungibleToken>

        //call SyncKeyMappingHandler for SyncKey Mapping called at buyers side
        subFlow(SyncKeyMappingFlowHandler(otherSide))

        //Get buyers and sellers account infos
        val senderAccountInfo = accountService.accountInfo(senderAccountName)[0].state.data
        val receiverAccountInfo = accountService.accountInfo(receiverAccountName)[0].state.data

        //Generate new keys for buyers and sellers
        val senderAccount = subFlow(RequestKeyForAccount(senderAccountInfo))
        val receiverAccount = subFlow(RequestKeyForAccount(receiverAccountInfo))

        //building transaction
        val notary = serviceHub.networkMapCache.notaryIdentities[0]
        val txBuilder = TransactionBuilder(notary)

        //part2 of DVP is to transfer cash - fungible token from buyer to seller and return the change to buyer
        addMoveTokens(txBuilder, inputs, moneyReceived)

        //add signers
        val signers: MutableList<AbstractParty> = ArrayList()
        signers.add(senderAccount)
        signers.add(receiverAccount)

        for ((state) in inputs) {
            signers.add(state.data.holder)
        }

        //sync keys with buyer, again sync for similar members
        subFlow(SyncKeyMappingFlow(otherSide, signers))

        //call filterMyKeys to get the my signers for seller node and pass in as a 4th parameter to CollectSignaturesFlow.
        //by doing this we tell CollectSignaturesFlow that these are the signers which have already signed the transaction
        val commandWithPartiesList: List<CommandWithParties<CommandData>> = txBuilder.toLedgerTransaction(serviceHub).commands

        val mySigners: MutableList<PublicKey> = ArrayList()
        commandWithPartiesList.map {
            val signer = (serviceHub.keyManagementService.filterMyKeys(it.signers) as ArrayList<PublicKey>)
            if(signer.size >0){
                mySigners.add(signer[0]) }
        }

        val selfSignedTransaction = serviceHub.signInitialTransaction(txBuilder, mySigners)
        val fullySignedTx = subFlow(CollectSignaturesFlow(selfSignedTransaction, listOf(otherSide), mySigners))

        //call FinalityFlow for finality
        return subFlow(FinalityFlow(fullySignedTx, Arrays.asList(otherSide)))
    }
}
