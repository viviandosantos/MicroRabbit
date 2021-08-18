using MicroRabbit.Banking.Application.Interfaces;
using MicroRabbit.Banking.Application.Models;
using MicroRabbit.Banking.Domain.Commands;
using MicroRabbit.Banking.Domain.Interfaces;
using MicroRabbit.Banking.Domain.Models;
using MicroRabbit.Core.Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroRabbit.Banking.Application.Services
{
    public class AccountService : IAccountService
    {
        private readonly IAccountRepository _repository;
        private readonly IEventBus _bus;

        public AccountService(IAccountRepository repository, IEventBus bus)
        {
            _repository = repository;
            _bus = bus;
        }

        public IEnumerable<Account> GetAccounts()
        {
            return _repository.GetAccounts();
        }

        public void Transfer(AccountTransfer accountTransfer) 
        {
            var command = new CreateTransferCommand(accountTransfer.From, accountTransfer.To, accountTransfer.Amount);
            _bus.SendCommand(command);
        }
    }
}
